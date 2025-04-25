defmodule DistributedKVStore.MerkleTree do

    defmodule Node do
        defstruct [:left, :right, :hash, :range]
        @type t :: %__MODULE__{
            left: t | nil,
            right: t | nil,
            hash: binary,
            range: {any, any}
        }
    end

    def build(list_kv) when is_list(list_kv) do
        leaves =
            for {k, v} <- list_kv do
                bi_payload = :erlang.term_to_binary({k, v})
                h = :crypto.hash(:sha256, bi_payload)
                %Node{left: nil, right: nil, hash: h, range: {k, k}}
            end
        build_tree(leaves)
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
                    range: {elem(left.range, 0), elem(right.range, 1)}
                }
            end)
        |> build_tree()
    end

    @spec diff(Node.t(), Node.t()) :: [{any,any}]
    def diff(%Node{hash: h}, %Node{hash: h}), do: []
    def diff(%Node{left: nil, right: nil, range: rg1}, %Node{left: nil, right: rg2}), do: [range]
    def diff(%Node{left: l1, right: r1}, %Node{left: l2, right: r2}) do
        diff(l1, l2) ++ diff(r1, r2)
    end
end