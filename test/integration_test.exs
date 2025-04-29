defmodule IntegrationTest do
  use ExUnit.Case, async: false
  alias DistributedKVStore.{ConsistentHashing, MerkleSync}

  @tokens_per_node 5
  @num_node 10
  @app Mix.Project.config()[:app]

  setup_all do
    LocalCluster.start()

    nodes = LocalCluster.start_nodes("kv", @num_node, connect: :all, verbose: true, timeout: 5000)
    all_nodes = [Node.self() | nodes]
    DistributedKVStore.initialize_nodes(nodes)
    IO.inspect(Node.list(), label: "Nodes in the cluster")

    Enum.each(nodes, fn n ->
      {:ok, _} = :rpc.call(n, Application, :ensure_all_started, [@app])
    end)

    ring = ConsistentHashing.build_ring(all_nodes, @tokens_per_node)

    {:ok, nodes: all_nodes, ring: ring, client: hd(all_nodes)}
  end

  defp put(node, ring, k, v),
    do: :rpc.call(node, DistributedKVStore, :put, [ring, k, v])

  defp get(node, ring, k),
    do: :rpc.call(node, DistributedKVStore, :get, [ring, k])

  test "store and retrieve key-value pair", %{ring: ring} do
    key = "user1"
    value = "Alice"

    assert :ok == DistributedKVStore.put(ring, key, value)
    assert ^value = DistributedKVStore.get(ring, key)
  end

  test "handle concurrent updates", %{ring: ring} do
    key = "user2"
    value1 = "Bob"
    value2 = "Charlie"

    # Store the first value
    assert :ok == DistributedKVStore.put(ring, key, value1)
    assert :ok == DistributedKVStore.put(ring, key, value2)

    # Retrieve the value and check if it's one of the updates
    value = DistributedKVStore.get(ring, "user2")
    assert value in [value1, value2]
  end

  test "replication and consistency", %{ring: ring} do
    key = "user3"
    value = "Eve"

    assert :ok == DistributedKVStore.put(ring, key, value)
    assert ^value = DistributedKVStore.get(ring, key)
  end

  test "merkle tree sync after concurrent updates", %{ring: ring} do
    key = "user4"
    value1 = "Dave"
    value2 = "Eva"

    assert :ok == DistributedKVStore.put(ring, key, value1)
    assert :ok == DistributedKVStore.put(ring, key, value2)

    MerkleSync.sync()

    value = DistributedKVStore.get(ring, key)
    assert value in [value1, value2]
  end

  test "resolve version conflicts during get operation", %{ring: ring} do
    key = "user5"
    value1 = "Frank"
    value2 = "Grace"

    assert :ok == DistributedKVStore.put(ring, key, value1)
    assert :ok == DistributedKVStore.put(ring, key, value2)

    value = DistributedKVStore.get(ring, key)
    assert value in [value1, value2]
  end

  test "merkle tree conflict resolution with multiple concurrent updates", %{ring: ring} do
    key = "user6"
    value1 = "Helen"
    value2 = "Ivy"
    value3 = "Jack"

    assert :ok == DistributedKVStore.put(ring, key, value1)
    assert :ok == DistributedKVStore.put(ring, key, value2)
    assert :ok == DistributedKVStore.put(ring, key, value3)

    # DistributedKVStore.MerkleSync.do_sync(ring)

    value = DistributedKVStore.get(ring, key)
    assert value in [value1, value2, value3]
  end

  test "hints are stored when quorum is not met", %{ring: ring} do
    key = "user7"
    value = "Kevin"

    Application.put_env(:distributed_kv, :node_fail_mode, :always_fail)

    put_result = DistributedKVStore.put(ring, key, value)
    assert put_result == :error

    hints = :ets.tab2list(:hints)
    assert length(hints) > 0
  end
end
