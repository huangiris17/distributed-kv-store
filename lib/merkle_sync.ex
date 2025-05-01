defmodule DistributedKVStore.MerkleSync do
    use GenServer

    alias DistributedKVStore.{ConsistentHashing, MerkleTree, NodeKV}

    @rep_factor Application.compile_env(:distributed_kv_store, :replication_factor, 3)
    @sync_interval_ms 60_000

    def start_link(ring) do
        GenServer.start_link(__MODULE__, ring, name: __MODULE__)
    end

    def init(ring) do
        Process.send_after(self(), :sync, @sync_interval_ms)
        {:ok, ring}
    end

    def handle_info(:sync, ring) do
        sync(ring)
        Process.send_after(self(), :sync, @sync_interval_ms)
        {:noreply, ring}
    end

    def sync(ring) do
        nodes = get_ring_nodes(ring)

        Enum.each(nodes, fn node ->
            synchronize_node(ring, node)
        end)
    end

    def synchronize_node(ring, node) do
        IO.puts("Node syncing...")
        key_hashes_for_node = ConsistentHashing.get_key_hashes_for_node(ring, node)

        Enum.each(key_hashes_for_node, fn key_hash ->
            replicas = ConsistentHashing.get_nodes_from_hash(ring, key_hash, @rep_factor)
            Enum.each(replicas, fn replica ->
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
        NodeKV.get_merkle_tree(node)
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

    def sync do
        GenServer.call(__MODULE__, :manual_sync)
    end

    def handle_call(:manual_sync, _from, ring) do
        sync(ring)
        {:reply, :ok, ring}
    end
end