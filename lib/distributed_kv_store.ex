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

    alias DistributedKVStore.ConsistentHashing

    # Module attributes
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
    def put(ring, key, value, context \\ nil) do
        new_vector_clock = VectorClock.update(context, self())
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
        Enum.reduce(responses, fn {_node, resp}, {_best_node, best_resp} = acc ->
            best_sum = Enum.reduce(best_resp.vector_clock || [], 0, fn {_n, count}, acc -> acc + count end)
            curr_sum = Enum.reduce(resp.vector_clock || [], 0, fn {_n, count}, acc -> acc + count end)
            if curr_sum > best_sum, do: {nil, resp}, else: acc
        end)
        |> elem(1)
    end
end
