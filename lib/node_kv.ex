defmodule DistributedKVStore.NodeKV do

    use GenServer
    alias DistributedKVStore.{MerkleTree, NodeKVBehaviour}
    @behaviour NodeKVBehaviour

    def start_link(opts \\ []) do
        name = opts[:name]
        # IO.puts("#{name} starts linking")
        GenServer.start_link(__MODULE__, %{kv_map: %{}, merkle_tree: nil}, name: name)
    end

    @impl GenServer
    def init(state) do
        # IO.puts("GenServer init ends")
        {:ok, %{state | merkle_tree: MerkleTree.build(state.kv_map)}}
    end

    @impl NodeKVBehaviour
    def get(node, key) do
        # IO.inspect(node, label: "Node called for get")
        GenServer.call(node, {:get, key})
    end

    @impl NodeKVBehaviour
    def get_all(node) do
        GenServer.call(node, :get_all)
    end

    @impl NodeKVBehaviour
    def put(node, key, value, vector_clock, timestamp) do
        GenServer.call(node, {:put, key, value, vector_clock, timestamp})
    end

    @impl NodeKVBehaviour
    def get_merkle_tree(node) do
        GenServer.call(node, :get_merkle_tree)
    end

    @impl GenServer
    def handle_call(message, _from, state) do
        current_node = Node.self()
        IO.puts("Handling message: #{inspect(message)} from #{current_node}")
        case message do
            {:get, key} ->
                result = Map.get(state.kv_map, key)
                # IO.puts("NodeKV: Retrieved value for key: #{key} => #{inspect(result)}")
                {:reply, result, state}

            :get_all ->
                {:reply, state.kv_map, state}

            {:put, key, value, vector_clock, timestamp} ->
                try do
                    new_kv_map = Map.put(state.kv_map, key, {value, vector_clock, timestamp})
                    new_merkle_tree = MerkleTree.build(new_kv_map)
                    new_state = %{state | kv_map: new_kv_map, merkle_tree: new_merkle_tree}

                    {:reply, {:ok, value}, new_state}
                rescue
                    exception ->
                        IO.puts("Error during put operation: #{exception}")
                        {:reply, :put_failed, state}
                end

            :get_merkle_tree ->
                {:reply, state.merkle_tree, state}

            _ ->
                {:reply, {:error, :unknown_call}, state}
        end
    end
end