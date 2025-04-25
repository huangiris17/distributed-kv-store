defmodule DistributedKVStore.MerkleSync do
    use GenServer

    alias DistributedKVStore.{ConsistentHashing, MerkleTree}

    @rep_factor Application.get_env(:distributed_kv_store, :replication_factor)
    @sync_interval_ms 60_000

    def start_link(ring) do
        GenServer.start_link(__MODULE__, ring, name: __MODULE__)
    end

    def init({ring, interval}) do
        Process.send_after(self(), :sync, @sync_interval_ms)
        {:ok, ring}
    end

    def handle_info(:sync, ring) do
        do_sync(ring)
        Process.send_after(self(), :sync, @sync_interval_ms)
        {:noreply, ring}
    end

    defp do_sync(ring) do
        for node <- get_ring_nodes(ring) do
            current_merkle = MerkleTree.build(node)

            keys_for_node = ConsistentHashing.get_keys_for_node(ring, node)

            Enum.each(keys_for_node, fn key ->
                replica_nodes = ConsistentHashing.get_nodes(ring, key, @rep_factor)
                Enum.each(replica_nodes, fn replica_node ->
                    remote_merkle = get_remote_merkle(replica_node)
                    case MerkleTree.diff(local_merkle, remote_merkle) do
                        [] -> :ok
                        differences -> propagate_changes(differences, replica_node)
                    end
                end)
            end)
        end
    end

    defp get_ring_nodes(ring), do ConsistentHashing.nodes(ring)

    defp get_remote_merkle(node) do
        send(node, {:get_merkle_tree, self()})
        receive do
            {:ok, merkle_tree} -> merkle_tree
        after
            5000 -> :error
        end
    end

    defp propagate_changes(differences, node) do
        Enum.each(differences, fn {k, v} ->
            send(node, {:update_merkle, k, v})
        end)
    end
end