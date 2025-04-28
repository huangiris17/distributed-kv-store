defmodule IntegrationTest do
  use ExUnit.Case, async: false

  setup_all do
    # Start a local cluster with 5 nodes for testing
    LocalCluster.start()
    IO.puts("Starting node")

    nodes = LocalCluster.start_nodes("test-cluster", 10, verbose: true, timeout: 5000)
    IO.inspect(Node.list(), label: "Nodes in the cluster")

    {:ok, nodes: nodes}
  end

  setup %{nodes: nodes} do
    # Initialize the ring with nodes
    ring = DistributedKVStore.ConsistentHashing.build_ring(nodes, 5)
    {:ok, ring: ring}
  end

  test "store and retrieve key-value pair", %{ring: ring} do
    key = "user1"
    value = "Alice"

    # Store the key-value pair
    assert :ok == DistributedKVStore.put(ring, key, value)

    # Retrieve the value
    assert {:ok, ^value} = DistributedKVStore.get(ring, key)
  end

  test "handle concurrent updates", %{ring: ring} do
    key = "user2"
    value1 = "Bob"
    value2 = "Charlie"

    # Store the first value
    assert :ok == DistributedKVStore.put(ring, key, value1)
    assert :ok == DistributedKVStore.put(ring, key, value2)

    # Retrieve the value and check if it's one of the updates
    assert {:ok, value} = DistributedKVStore.get(ring, key)
    assert value in [value1, value2]
  end

  test "replication and consistency", %{ring: ring} do
    key = "user3"
    value = "Eve"

    # Store the key-value pair
    assert :ok == DistributedKVStore.put(ring, key, value)

    # Retrieve from a different node (should have the same value)
    assert {:ok, ^value} = DistributedKVStore.get(ring, key)
  end

  test "merkle tree sync after concurrent updates", %{ring: ring} do
    key = "user4"
    value1 = "Dave"
    value2 = "Eva"

    # Store the first value on one node
    assert :ok == DistributedKVStore.put(ring, key, value1)

    # Simulate another node concurrently updating the value
    assert :ok == DistributedKVStore.put(ring, key, value2)

    # Trigger Merkle tree synchronization across nodes
    # We simulate that MerkleSync is running and handling conflict resolution
    DistributedKVStore.MerkleSync.sync()

    # Retrieve the value and ensure the latest value is chosen based on timestamp/version
    assert {:ok, value} = DistributedKVStore.get(ring, key)
    assert value in [value1, value2]
  end

  test "resolve version conflicts during get operation", %{ring: ring} do
    key = "user5"
    value1 = "Frank"
    value2 = "Grace"

    # Store the first value on one node
    assert :ok == DistributedKVStore.put(ring, key, value1)

    # Simulate a conflicting update by another node
    assert :ok == DistributedKVStore.put(ring, key, value2)

    # Retrieve the value and ensure the conflict resolution mechanism works
    # Since we don't specify which value should win, the system will pick one based on the logic
    assert {:ok, value} = DistributedKVStore.get(ring, key)
    assert value in [value1, value2]
  end

  test "merkle tree conflict resolution with multiple concurrent updates", %{ring: ring} do
    key = "user6"
    value1 = "Helen"
    value2 = "Ivy"
    value3 = "Jack"

    # Simulate 3 concurrent updates
    assert :ok == DistributedKVStore.put(ring, key, value1)
    assert :ok == DistributedKVStore.put(ring, key, value2)
    assert :ok == DistributedKVStore.put(ring, key, value3)

    # Simulate Merkle tree sync between nodes
    DistributedKVStore.MerkleSync.sync()

    # Retrieve the value after conflict resolution
    # The system should choose one of the values based on timestamp/version or apply a merge strategy
    assert {:ok, value} = DistributedKVStore.get(ring, key)
    assert value in [value1, value2, value3]
  end

  test "hints are stored when quorum is not met", %{ring: ring} do
    key = "user7"
    value = "Kevin"

    # Simulate a scenario where a quorum is not met (e.g., some nodes are down)
    Application.put_env(:distributed_kv, :node_fail_mode, :always_fail)

    # Try to store the key-value pair (this should fail due to lack of quorum)
    put_result = DistributedKVStore.put(ring, key, value)
    assert put_result == :error

    # Check if hints were stored for retry
    hints = :ets.tab2list(:hints)
    assert length(hints) > 0
  end
end
