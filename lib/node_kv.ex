defmodule DistributedKVStore.NodeKV do
    def start(opts \\ []) do
        Agent.start_link(fn -> %{} end, opts)
    end

    @spec get(pid() | atom(), any()) :: {any(), map()} | nil
    def get(node, key) do
        Agent.get(node, fn db -> Map.get(db, key) end)
    end

    @spec put(pid() | atom(), any(), any(), map(), any()) :: {:ok, any()}
    def put(node, key, val, vector_clock, timestamp) do
        Agent.update(node, fn db -> Map.put(db, key, {val, vector_clock, timestamp}) end)
        {:ok, val}
    end

end