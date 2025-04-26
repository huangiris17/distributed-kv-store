defmodule DistributedKVStore.MerkleTree do

    defmodule Node do
        defstruct [:left, :right, :hash, :range, :key, :value]
        @type t :: %__MODULE__{
            left: t | nil,
            right: t | nil,
            hash: binary,
            range: {any, any},
            key: any,
            value: any
        }
    end

    def build(kv) when is_map(kv) do
        build(Map.to_list(kv))
    end

    def build(kv) when is_list(kv) do
        leaves =
            kv
            |> Enum.sort_by(fn {k, _v} -> k end)
            |> Enum.map(fn {k, v} ->
                binary_payload = :erlang.term_to_binary({k, v})
                hash = :crypto.hash(:sha256, binary_payload)
                %Node{
                    left: nil,
                    right: nil,
                    hash: hash,
                    range: {k, k},
                    key: k,
                    value: v
                }
            end)

        case leaves do
            [] -> %Node{hash: :crypto.hash(:sha256, "empty"), range: {nil, nil}}
            _ -> build_tree(leaves)
        end
    end

    defp build_tree([node]), do: node
    defp build_tree(nodes) do
        nodes
        |> Enum.chunk_every(2, 2, [List.last(nodes)])
        |> Enum.map(fn
            [left, right] ->
                combined_hash = :crypto.hash(:sha256, left.hash <> right.hash)
                %Node{
                    left: left,
                    right: right,
                    hash: combined_hash,
                    range: {min_key(left.range, right.range), max_key(left.range, right.range)}
                }
            [single] -> single
        end)
        |> build_tree()
    end

    defp min_key({nil, _}, {b, _}), do: b
    defp min_key({a, _}, {nil, _}), do: a
    defp min_key({a, _}, {b, _}) when a <= b, do: a
    defp min_key(_, {b, _}), do: b

    defp max_key({_, nil}, {_, b}), do: b
    defp max_key({_, a}, {_, nil}), do: a
    defp max_key({_, a}, {_, b}) when a >= b, do: a
    defp max_key(_, {_, b}), do: b

    @spec diff(Node.t(), Node.t()) :: [{any,any}]
    def diff(%Node{hash: h}, %Node{hash: h}), do: []

    def diff(%Node{left: nil, right: nil, key: k, value: v}, %Node{left: nil, right: nil, key: k, value: _}) do
        [{k, v}]
    end

    def diff(%Node{left: nil, right: nil, key: k1, value: v1}, %Node{left: nil, right: nil, key: k2, value: _}) when k1 != k2 do
        [{k1, v1}]
    end

    def diff(%Node{left: l1, right: r1, hash: h1}, %Node{left: l2, right: r2, hash: h2})
        when h1 != h2 and not is_nil(l1) and not is_nil(l2) and not is_nil(r1) and not is_nil(r2) do
            left_diff = diff(l1, l2)
            right_diff = diff(r1, r2)
            left_diff ++ right_diff
    end

    def diff(%Node{left: l1, right: r1}, %Node{left: l2, right: r2}) do
        l_diff = case {l1, l2} do
            {nil, nil} -> []
            {nil, node} -> get_all_keys(node)
            {node, nil} -> get_all_keys(node)
            {l1, l2} -> diff(l1, l2)
        end

        r_diff = case {r1, r2} do
            {nil, nil} -> []
            {nil, node} -> get_all_keys(node)
            {node, nil} -> get_all_keys(node)
            {r1, r2} -> diff(r1, r2)
        end

        l_diff ++ r_diff
    end

    def diff(%Node{} = node1, %Node{} = node2) do
        get_all_keys(node1) ++ get_all_keys(node2)
    end

    def get_all_keys(%Node{left: nil, right: nil, key: k, value: v}) when not is_nil(k), do: [{k, v}]
    def get_all_keys(%Node{left: l, right: r}) do
        left_keys = if l, do: get_all_keys(l), else: []
        right_keys = if r, do: get_all_keys(r), else: []
        left_keys ++ right_keys
    end
    def get_all_keys(_), do: []
end