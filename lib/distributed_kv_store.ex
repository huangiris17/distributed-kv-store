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
        merge_versions(responses)
    end

    #put/4
    def put(ring, key, value, vector_clock \\ nil) do
        # Update current vector clock on current node
        new_vector_clock = VectorClock.update(vector_clock, self())
        nodes = ConsistentHashing.get_nodes(ring, key, @replication_factor)
        ref = make_ref()

        Enum.each(nodes, fn node ->
            spawn(fn ->
                result = node_put(node, key, value, new_vector_clock)
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
                    {:error, _} -> HintedHandoff.store_hint(node, key, value, new_vector_clock)
                    _ -> :ok
                end
            end)

            :error
        end
    end

    # Make a simulated remote get call to a node
    defp node_get(node, key) do
        NodeKV.get(key, node)
    rescue
        _ -> {:error, :node_down}
    end

    # Make a simulated remote put call to a node
    defp node_put(node, key, value, vector_clock) do
        NodeKV.put(key, value, vector_clock, node)
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
    defp merge_versions([]), do: {:error, :no_responses}
    defp merge_versions([{_node, single}]), do: single
    defp merge_versions(responses) do
        versions = Enum.map(responses, fn {_node, resp} -> resp end)

        case pick_latest_version(versions) do
          {:conflict, concurrent_versions} ->
            {:conflict, concurrent_versions}
          latest ->
            latest
        end
    end

    defp pick_latest_version(versions) do
        candidate =
            Enum.find(versions, fn version ->
                Enum.all?(versions, fn other ->
                    case VectorClock.compare(version.vector_clock, other.vector_clock) do
                    :descendant -> true       # version is later than other
                    :equal -> true            # or they are the same
                    :ancestor -> false        # version is older than other -> candidate fails
                    :concurrent -> false      # versions are concurrent -> candidate fails
                    end
                end)
            end)

        if candidate do
            candidate
        else
            {:conflict, versions}
        end
    end
end
