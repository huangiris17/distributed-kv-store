defmodule DistributedKVStore.NodeKVBehaviour do
    @callback get(pid() | atom(), any()) :: any()
    @callback get_all(pid() | atom()) :: map()
    @callback put(pid() | atom(), any(), any(), any(), any()) :: {:ok, any()} | :put_failed
    @callback get_merkle_tree(pid() | atom()) :: any()
  end