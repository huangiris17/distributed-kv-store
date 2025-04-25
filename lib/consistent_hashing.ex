defmodule DistributedKVStore.ConsistentHashing do
    @moduledoc " A Module for managing consistent hashing ring."

    import Bitwise

    defmodule Token do
      defstruct value: nil, node: nil
    end

    # build_ring/2
    # Returns a consistent hashing ring with total number of token: number of node * tokens_per_node
    def build_ring(node_list, tokens_per_node) do
      node_list
      |> Enum.flat_map(fn node -> generate_tokens(node, tokens_per_node) end)
      |> Enum.sort_by(fn tuple -> tuple.value end)
    end

    # generate_tokens/2
    # Generate a list of tuples of {hashed token value, node}
    def generate_tokens(node, count) do
      1..count
      |> Enum.map(fn i ->
        token_value = compute_token("#{node}-#{i}")
        %Token{value: token_value, node: node}
      end)
    end

    def nodes(ring) do
      ring
      |> Enum.map(& &1.node)
      |> Enum.uniq()
    end

    # get_node/2
    # Get the node that is in charge of the key on the ring
    def get_node(ring, key) do
      key_hash = compute_token(key)
      case Enum.find(ring, fn token -> token.value >= key_hash end) do
        nil ->
          # key_hash is greater than any token; wrap around to the first token
          hd(ring).node
        token ->
          token.node
      end
    end

    # get_node/3
    # Get the list of nodes that are in charge of the key on the ring
    def get_nodes(ring, key, replication_factor \\ 3) do
      key_hash = compute_token(key)
      start_index = Enum.find_index(ring, fn token -> token.value >= key_hash end) || 0

      ring
      |> Stream.cycle()
      |> Enum.slice(start_index, replication_factor)
      |> Enum.map(fn tuple -> tuple.node end)
      |> Enum.uniq()  # Remove duplicates if a single node has multiple tokens
    end

    def get_keys_for_node(ring, node) do
      node_tokens = Enum.filter(ring, fn %Token{node: n} -> n == node end)

      node_tokens
      |> Enum.map(fn %Token{value: token_value} ->
          next_token = get_next_token(ring, token_value)
          {token_value, next_token}
      end)
      |> Enum.flat_map(fn {start_value, end_value} ->
          Enum.map(start_value..end_value, &compute_key(&1))
      end)
    end

    defp get_next_token(ring, token_value) do
      ring
      |> Enum.find(fn %Token{value: value} -> value > token_value end)
      |> case do
           nil -> hd(ring).value
           %Token{value: next_value} -> next_value
         end
    end

    # compute_token/2
    # Compute the hashed value of the key
    # Modulo is implemented for fixed ring size
    def compute_token(key, modulo \\ 4_294_967_295) do
      :crypto.hash(:sha, key)
      |> :binary.bin_to_list()
      |> Enum.reduce(0, fn byte, acc -> (acc <<< 8) + byte end)
      |> rem(modulo)
    end

    defp compute_key(token_value) do
      key_hash = :crypto.hash(:sha256, <<token_value::binary>>)
      :binary.encode_unsigned(key_hash)
    end

    def handle_cast({:update_merkle, k, v}, state) do
      new_data = Map.put(state.data, k, v)

      new_merkle_tree = MerkleTree.build(new_data)

      {:noreply, %{state | data: new_data, merkle_tree: new_merkle_tree}}
    end
  end