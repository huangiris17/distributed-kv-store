defmodule DistributedKVStore do
    @moduledoc """
    Implemented distributed key–value store core.

    - get/2: Retrieve the value for a given key from replica nodes.
    - put/4: Write a value (with an updated vector clock) to replica nodes.

    Replica coordination is achieved by spawning plain processes that communicate via messages.
    If a node fails to respond or returns an error, a hint is stored in ETS for a later retry.
    """

    # Sub-module alias
    alias DistributedKVStore.{ConsistentHashing, VectorClock, NodeKV, Gossip, HintedHandoff}

    # Module attribute
    @replication_factor 3
    @write_quorum 2

    def start_node(node_name) do
        {:ok, pid} = NodeKV.start_link(name: node_name)
        IO.puts("#{node_name} started with PID: #{inspect(pid)}")

        pid
    end

    def initialize_nodes(nodes) do
        Enum.each(nodes, &start_node/1)

        HintedHandoff.init()

        now = System.system_time(:millisecond)
        initial_view =
            for n <- nodes, into: %{} do
                {n, %{status: :alive, timestamp: now}}
            end

        Enum.each(nodes, fn node_name ->
            spawn(fn -> Gossip.start(node_name, initial_view) end)
        end)
    end

    # get/2
    def get(ring, key) do
        nodes = ConsistentHashing.get_nodes(ring, key, @replication_factor)
        ref = make_ref()

        Enum.each(nodes, fn node ->
            parent = self()
            spawn(fn ->
                result = node_get(node, key)
                send(parent, {ref, node, result})
            end)
        end)

        responses = collect_responses(ref, length(nodes))
        valid_responses = filter_success_responses(responses)
        merge_versions(ring, key, valid_responses)
    end

    #put/4
    def put(ring, key, value, vector_clock \\ nil) do
        nodes = ConsistentHashing.get_nodes(ring, key, @replication_factor)
        IO.inspect(nodes, label: "Nodes are responsible")
        timestamp = System.system_time(:millisecond)
        ref = make_ref()
        parent = self()

        Enum.each(nodes, fn node ->
            spawn(fn ->
                vector_clock = if vector_clock == nil do
                    get_or_create_vector_clock(node, key)
                else
                    vector_clock
                end
                result = node_put(node, key, value, vector_clock, timestamp)
                send(parent, {ref, node, result})
            end)
        end)

        responses = collect_responses(ref, length(nodes))
        success_responses = filter_success_responses(responses)
        ack_count = length(success_responses)

        if ack_count >= @write_quorum do
            IO.puts("Response counts > quorum")
            :ok
        else
            IO.puts("Response counts < quorum")
            # If quorum is not met, for every failed response, store a hint
            Enum.each(responses, fn {node, res} ->
                case res do
                    {:error, _} ->
                        vector_clock = if vector_clock == nil do
                            get_or_create_vector_clock(node, key)
                        else
                            vector_clock
                        end
                        HintedHandoff.store_hint(node, key, value, vector_clock)
                    _ -> :ok
                end
            end)

            :error
        end
    end

    # Make a simulated remote get call to a node
    defp node_get(node, key) do
        try do
            result = NodeKV.get(node, key)
            # IO.puts("KV_Store: Retrieved #{node} value for key: #{key} => #{inspect(result)}")
            case result do
            {value, vector_clock, timestamp} -> {:ok, {value, vector_clock, timestamp}}
            {:error, _reason} -> {:error, :key_not_found}
            end
        rescue
            _ -> {:error, :node_down}
        end
    end

    # Make a simulated remote put call to a node
    defp node_put(node, key, value, vector_clock, timestamp) do
        # NodeKV.put(node, key, value, vector_clock, timestamp)
        case Application.get_env(:distributed_kv, :node_fail_mode, :always_succeed) do
            :always_fail ->
                # Simulate node failure
                IO.puts("Simulating node failure for #{inspect(node)}")
                {:error, :node_down}

            :partial ->
                # Simulate partial failure
                if node in [:node1, :node2, :node4, :node5] do
                    IO.puts("Simulating failure for node #{inspect(node)}")
                    {:error, :node_down}
                else
                    result = NodeKV.put(node, key, value, vector_clock, timestamp)
                    case result do
                        {:put_failed} -> {:error, :put_failed}
                        {:ok, value} -> {:ok, value}
                    end
                end

            _ ->
                result = NodeKV.put(node, key, value, vector_clock, timestamp)
                case result do
                    {:put_failed} -> {:error, :put_failed}
                    {:ok, value} -> {:ok, value}
                end
        end
    rescue
        _ -> {:error, :node_down}
    end

    # Collect responses from spawned processes based on the given reference
    defp collect_responses(ref, count), do: collect_responses(ref, count, [])
    defp collect_responses(_ref, 0, acc), do: acc
    defp collect_responses(ref, count, acc) do
        # IO.puts("Collecting responses - #{count}..")
        receive do
            {^ref, node, response} ->
                collect_responses(ref, count - 1, [{node, response} | acc])
        after
            5000 ->
                acc
        end
    end

    # Merge the responses from replicas by comparing vector clocks
    defp merge_versions(_ring, _key, []) do
        IO.puts("merged version return error")
        {:error, :no_responses}
    end
    defp merge_versions(_ring, _key, [{_node, single}]), do: single
    defp merge_versions(ring, key, responses) do
        versions = Enum.map(responses, fn {_node, {:ok, resp}} -> resp end)

        case pick_version(versions) do
            {:conflict, concurrent_versions} ->
                {val, merged_vc} = pick_version_lww(concurrent_versions)
                put(ring, key, val, merged_vc)
                val
            {:ok, {val, _vc}} ->
                val
        end
    end

    # Pick the latest version by comparing all version pairs
    defp pick_version(versions) do
        candidate =
            Enum.find(versions, fn {_val1, vc1, _ts1} ->
                Enum.all?(versions, fn {_val2, vc2, _ts2} ->
                    case VectorClock.compare(vc1, vc2) do
                    :descendant -> true       # version is later than other
                    :equal -> true            # or they are the same
                    :ancestor -> false        # version is older than other -> candidate fails
                    :concurrent -> false      # versions are concurrent -> candidate fails
                    end
                end)
        end)

        if candidate do
            {:ok, candidate}
        else
            {:conflict, versions}
        end
    end

    defp pick_version_lww(versions) do
        {val, _vs, _ts} = Enum.max_by(versions, fn {_val, _vs, ts} -> ts end)

        merged_clock =
            versions
            |> Enum.map(fn {_val, vs, _ts} -> vs end)
            |> Enum.reduce(%{}, &VectorClock.merge(&2, &1))

        {val, merged_clock}
    end

    defp get_or_create_vector_clock(node, key) do
        case node_get(node, key) do
            {:ok, {_value, vector_clock, _timestamp}} ->
                # IO.puts("Found existing vector clock for #{key}: #{inspect(vector_clock)}")
                VectorClock.update(vector_clock, node)

            {:error, _} ->
                # IO.puts("Creating a new vector clock for #{key}")
                VectorClock.update(nil, node)
        end
    end

    defp filter_success_responses(responses) do
        Enum.filter(responses, fn {_node, response} ->
            case response do
                {:error, _} -> false
                {:ok, _res} -> true
                _ -> false
            end
        end)
    end
end
