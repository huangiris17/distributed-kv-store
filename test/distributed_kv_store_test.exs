ExUnit.start()

defmodule DistributedKVStoreTest do
  use ExUnit.Case

  alias DistributedKVStore.{ConsistentHashing, VectorClock, NodeKV, MerkleTree, HintedHandoff, MerkleSync}

  @nodes [:node1, :node2, :node3, :node4, :node5, :node6, :node7, :node8, :node9, :node10]
  @key "test_key"
  @value "test_value"
  @replication_factor 3

  setup_all do
    Enum.each(@nodes, fn node_name ->
      Node.start(:"#{node_name}@127.0.0.1")
    end)

    Enum.each(@nodes, fn node_name ->
      Enum.each(@nodes, fn other_node_name ->
        unless node_name == other_node_name do
          Node.connect(:"#{other_node_name}@127.0.0.1")
        end
      end)
    end)

    DistributedKVStore.initialize_nodes(@nodes)

    IO.puts("Connected nodes: #{inspect(Node.list())}")
    {:ok, nodes: @nodes}
  end

  setup do
    ring = ConsistentHashing.build_ring(@nodes, 10)
    if :ets.info(:hints) != :undefined, do: :ets.delete_all_objects(:hints)
    {:ok, ring: ring}
  end

  @doc """
  Scenario 1: All nodes succeed.

  We force NodeKV to always succeed via application env.
  Then, a put should return :ok and a subsequent get should yield
  a valid map with a value and vector_clock.
  """
  test "successful put and get when all nodes succeed", %{ring: ring} do
    Application.put_env(:distributed_kv, :node_fail_mode, :always_succeed)

    put_result = DistributedKVStore.put(ring, @key, @value)
    assert put_result == :ok

    get_result = DistributedKVStore.get(ring, @key)
    assert get_result == @value
  end

  @doc """
  Scenario 2: All nodes fail.

  Forcing NodeKV to always fail, the put should return :error
  (quorum not met) and get should yield {:error, :no_responses}.
  """
  test "failed put and get when all nodes fail", %{ring: ring} do
    Application.put_env(:distributed_kv, :node_fail_mode, :always_fail)

    put_result = DistributedKVStore.put(ring, "key_fail", "value_fail")
    assert put_result == :error

    get_result = DistributedKVStore.get(ring, "key_fail")
    assert get_result == {:error, :no_responses}
  end

  @doc """
  Scenario 3: Partial failure with quorum met.

  In :partial mode, two out of three nodes succeed. With a quorum set to 2,
  the put should succeed and get should return a valid response.
  """
  test "partial failure with quorum met", %{ring: ring} do
    Application.put_env(:distributed_kv, :node_fail_mode, :partial)

    put_result = DistributedKVStore.put(ring, "key_partial", "value_partial")
    assert put_result == :ok

    get_result = DistributedKVStore.get(ring, "key_partial")
    assert get_result == "value_partial"
  end

  @doc """
  Scenario 4: Partial failure with quorum not met.

  Here we force more failures than permitted and thereby not meeting quorum.
  In that case, the put should return :error and hints should be stored.
  """
  test "partial failure with quorum not met", %{ring: ring} do
    Application.put_env(:distributed_kv, :node_fail_mode, :always_fail)

    put_result = DistributedKVStore.put(ring, "key_no_quorum", "value_no_quorum")
    assert put_result == :error

    # Check hints were stored
    hints = :ets.tab2list(:hints)
    assert length(hints) > 0
  end

  test "get with missing key", %{ring: ring} do
    get_result = DistributedKVStore.get(ring, "non_existent_key")
    assert get_result == {:error, :no_responses}
  end

  test "successful get after hint stored", %{ring: ring} do
    Application.put_env(:distributed_kv, :node_fail_mode, :always_fail)

    put_result = DistributedKVStore.put(ring, @key, @value)
    assert put_result == :error

    hints_before = :ets.tab2list(:hints)
    assert Enum.any?(hints_before, fn {node, key, value, _vc, _cnt} ->
      key == @key and value == @value
    end)
    assert length(hints_before) == @replication_factor

    # Simulate retry by forcing a success for the hint
    Application.put_env(:distributed_kv, :node_fail_mode, :always_succeed)

    HintedHandoff.retry_hints()

    get_result = DistributedKVStore.get(ring, @key)
    assert get_result == @value

    hints_after = :ets.tab2list(:hints)
    refute Enum.any?(hints_after, fn {node, key, value, _vc, _cnt} ->
      key == @key and value == @value
    end)
  end

  @doc """
  Merkle tree sync resolves divergent replicas.

  We simulate two replicas diverging on a shared key. After running MerkleSync.sync(),
  the out-of-sync replica should receive the correct value.
  """
  test "merkle tree sync resolves inconsistent replicas", %{ring: ring} do
    Application.put_env(:distributed_kv, :node_fail_mode, :always_succeed)

    # Pick a test key and determine responsible nodes
    value1 = "original_value"
    value2 = "outdated_value"
    [node1, node2 | _] = ConsistentHashing.get_nodes(ring, @key, 2)

    # Put value1 to node1
    vc1 = VectorClock.update(nil, node1)
    {:ok, _val1} = NodeKV.put(node1, @key, value1, vc1, System.system_time(:millisecond))

    # Put stale value2 to node2
    vc2 = VectorClock.update(nil, node2)
    {:ok, _val2} = NodeKV.put(node2, @key, value2, vc2, System.system_time(:millisecond))

    # Verify nodes are diverged
    mt1 = NodeKV.get_merkle_tree(node1)
    mt2 = NodeKV.get_merkle_tree(node2)
    diffs = MerkleTree.diff(mt1, mt2)
    assert Enum.any?(diffs, fn {k, _} -> k == @key end)

    # Start MerkleSync manually
    MerkleSync.synchronize_node(ring, node1)

    # node2 should now be synced with node1
    {val, _, _} = NodeKV.get(node2, @key)
    assert val == value1
  end

end
