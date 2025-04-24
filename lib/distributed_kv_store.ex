###############################
# Distributed KV Store   #
###############################

defmodule DistributedKVStore do
    @moduledoc """
    Implemented distributed key–value store core.

    - get/2: Retrieve the value for a given key from replica nodes.
    - put/4: Write a value (with an updated vector clock) to replica nodes.

    Replica coordination is achieved by spawning plain processes that communicate via messages.
    If a node fails to respond or returns an error, a hint is stored in ETS for a later retry.
    """

    # Sub-module alias
    alias DistributedKVStore.ConsistentHashing
    alias DistributedKVStore.VectorClock
    alias DistributedKVStore.NodeKV

    # Module attribute
    @replication_factor 3
    @write_quorum 2

    # get/2
    def get(ring, key) do
        nodes = ConsistentHashing.get_nodes(ring, key, @replication_factor)
        ref = make_ref()

        Enum.each(nodes, fn node ->
            spawn(fn ->
                result = node_get(node, key)
                send(self(), {ref, node, result})
            end)
        end)

        responses = collect_responses(ref, length(nodes))
        merge_versions(ring, key, responses)
    end

    #put/4
    def put(ring, key, value, vector_clock \\ nil) do
        # Update current vector clock on current node
        # Every node carries its own version of vector clock
        new_vector_clock = VectorClock.update(vector_clock, self())
        update(ring, key, value, new_vector_clock)
    end

    defp update(ring, key, value, vector_clock) do
        nodes = ConsistentHashing.get_nodes(ring, key, @replication_factor)
        timestamp = System.system_time(:millisecond)
        ref = make_ref()

        Enum.each(nodes, fn node ->
            spawn(fn ->
                result = node_put(node, key, value, vector_clock, timestamp)
                send(self(), {ref, node, result})
            end)
        end)

        responses = collect_responses(ref, length(nodes))
        # Count the successful acknowledgments from the nodes
        ack_count = Enum.count(responses, fn {_node, res} ->
            case res do
                {:ok, _} -> true
                _ -> false
            end
        end)

        if ack_count >= @write_quorum do
            :ok
        else
            # If quorum is not met, for every failed response, store a hint
            Enum.each(responses, fn {node, res} ->
                case res do
                    {:error, _} -> HintedHandoff.store_hint(node, key, value, vector_clock, timestamp)
                    _ -> :ok
                end
            end)

            :error
        end
    end

    # Make a simulated remote get call to a node
    defp node_get(node, key) do
        NodeKV.get(node, key)
    rescue
        _ -> {:error, :node_down}
    end

    # Make a simulated remote put call to a node
    defp node_put(node, key, value, vector_clock, timestamp) do
        NodeKV.put(node, key, value, vector_clock, timestamp)
    rescue
        _ -> {:error, :node_down}
    end

    # Collect responses from spawned processes based on the given reference
    defp collect_responses(ref, count), do: collect_responses(ref, count, [])
    defp collect_responses(_ref, 0, acc), do: acc
    defp collect_responses(ref, count, acc) do
        receive do
            {^ref, node, response} ->
                collect_responses(ref, count - 1, [{node, response} | acc])
        after
            5000 ->
                acc
        end
    end

    # Merge the responses from replicas by comparing vector clocks
    defp merge_versions(_ring, _key, []), do: {:error, :no_responses}
    defp merge_versions(_ring, _key, [{_node, single}]), do: single
    defp merge_versions(ring, key, responses) do
        versions = Enum.map(responses, fn {_node, resp} -> resp end)

        case pick_version(versions) do
            {:conflict, concurrent_versions} ->
                {val, merged_vc} = pick_version_lww(concurrent_versions)
                update(ring, key, val, merged_vc)
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

        # If one reponse is equal or later than all other responses, return the response.
        # Otherwise, return all responses
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
end
