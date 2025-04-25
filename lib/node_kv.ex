defmodule DistributedKVStore.NodeKV

    alias DistributedKVStore.MerkleTree

    def start(opts \\ []) do
        Agent.start_link(fn -> %{kv_map: %{}, merkle_tree: nil} end, opts)
    end

    @spec get(pid() | atom(), any()) :: {any(), map()} | nil
    def get(node, key) do
        Agent.get(node, fn db -> Map.get(db[:kv_map], key) end)
    end

    @spec put(pid() | atom(), any(), any(), map(), any()) :: {:ok, any()}
    def put(node, key, val, vector_clock, timestamp) do
        Agent.update(node, fn db ->
            new_map = Map.put(db[:kv_map], key, {val, vector_clock, timestamp})
            new_merkle_tree = MerkleTree.build(new_map)
            %{db | kv_map: new_map, merkle_tree: new_merkle_tree}
        end)
        {:ok, val}
    end

    def get_merkle_tree(node) do
        Agent.get(node, fn db -> db[:merkle_tree] end)
    end

    def handle_cast({:update_merkle, k, v}, state) do
        new_kv_map = Map.put(state.data, k, v)

        new_merkle_tree = MerkleTree.build(new_data)

        {:noreply, %{state | kv_map: new_kv_map, merkle_tree: new_merkle_tree}}
      end
end