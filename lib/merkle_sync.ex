defmodule DistributedKVStore.MerkleSync do
    use GenServer

    alias DistributedKVStore.{ConsistentHashing, MerkleTree, NodeKV}

    @rep_factor Application.get_env(:distributed_kv_store, :replication_factor)
    @sync_interval_ms 60_000

    def start_link(ring) do
        GenServer.start_link(__MODULE__, ring, name: __MODULE__)
    end

    def init(ring) do
        Process.send_after(self(), :sync, @sync_interval_ms)
        {:ok, ring}
    end

    def handle_info(:sync, ring) do
        do_sync(ring)
        Process.send_after(self(), :sync, @sync_interval_ms)
        {:noreply, ring}
    end

    defp do_sync(ring) do
        nodes = get_ring_nodes(ring)

        Enum.each(nodes, fn node ->
            synchronize_node(ring, node)
        end)
    end

    defp synchronize_node(ring, node) do
        keys_for_node = ConsistentHashing.get_keys_for_node(ring, node)

        Enum.each(keys_for_node, fn key ->
            replica_nodes = ConsistentHashing.get_nodes(ring, key, @rep_factor)
            Enum.each(replica_nodes, fn replica ->
                if node != replica do
                    synchronize_key(node, replica)
                end
            end)
        end)
    end

    defp synchronize_key(source_node, target_node) do
        source_merkle = get_merkle_tree(source_node)
        target_merkle = get_merkle_tree(target_node)

        case source_merkle && target_merkle do
            true ->
                differences = MerkleTree.diff(source_merkle, target_merkle)

                if differences != [] do
                    resolve_differences(source_node, target_node, differences)
                end

            _ ->
                do_full_sync(source_node, target_node)
        end
    end

    defp get_ring_nodes(ring) do
        ConsistentHashing.nodes(ring)
    end

    defp get_merkle_tree(node) do
        send(node, {:get_merkle_tree, self()})
        receive do
            {:ok, merkle_tree} -> merkle_tree
        after
            5000 -> :error
        end
    end

    defp resolve_differences(source_node, target_node, differences) do
        Enum.each(differences, fn {key, _} ->
            case NodeKV.get(source_node, key) do
                {value, vector_clock, timestamp} ->
                    NodeKV.put(target_node, key, value, vector_clock, timestamp)
                nil ->
                    :ok
            end
        end)
    end

    defp do_full_sync(source_node, target_node) do
        all_data = NodeKV.get_all(source_node)

        Enum.each(all_data, fn {key, {value, vector_clock, timestamp}} ->
            NodeKV.put(target_node, key, value, vector_clock, timestamp)
        end)
    end
end