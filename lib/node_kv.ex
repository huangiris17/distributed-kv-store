defmodule DistributedKVStore.NodeKV do

    use GenServer
    alias DistributedKVStore.MerkleTree

    def start_link(opts \\ []) do
        GenServer.start_link(__MODULE__, fn -> %{kv_map: %{}, merkle_tree: nil} end, opts)
    end

    def init(state) do
        {:ok, %{state | merkle_tree: MerkleTree.build(state.kv_map)}}
    end

    def get(node, key) do
        GenServer.call(node, {:get, key})
    end

    def get_all(node) do
        GenServer.call(node, :get_all)
    end

    def put(node, key, value, vector_clock, timestamp) do
        GenServer.call(node, {:put, key, value, vector_clock, timestamp})
    end

    def get_merkle_tree(node) do
        GenServer.call(node, :get_merkle_tree)
    end

    def handle_call(message, _from, state) do
        case message do
            {:get, key} ->
                result = Map.get(state.kv_map, key)
                {:reply, result, state}

            :get_all ->
                {:reply, state.kv_map, state}

            {:put, key, value, vector_clock, timestamp} ->
                new_kv_map = Map.put(state.kv_map, key, {value, vector_clock, timestamp})
                new_merkle_tree = MerkleTree.build(new_kv_map)
                new_state = %{state | kv_map: new_kv_map, merkle_tree: new_merkle_tree}

                {:reply, {:ok, value}, new_state}

            :get_merkle_tree ->
                {:reply, state.merkle_tree, state}

            _ ->
                {:reply, {:error, :unknown_call}, state}
        end
    end
end