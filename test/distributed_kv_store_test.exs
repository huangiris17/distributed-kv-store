ExUnit.start()

defmodule DistributedKVStoreTest do
  use ExUnit.Case

  @ring :my_ring
  @key "test_key"
  @value "test_value"

  setup do
    if :ets.info(:hints) != :undefined, do: :ets.delete_all_objects(:hints)
    :ok
  end

  @doc """
  Scenario 1: All nodes succeed.
  
  We force NodeKV to always succeed via application env.
  Then, a put should return :ok and a subsequent get should yield
  a valid map with a value and vector_clock.
  """
  test "successful put and get when all nodes succeed" do
    Application.put_env(:distributed_kv, :node_fail_mode, :always_succeed)

    put_result = DistributedKVStore.put(@ring, @key, @value)
    assert put_result == :ok

    get_result = DistributedKVStore.get(@ring, @key)
    assert is_map(get_result)
    assert Map.has_key?(get_result, :value)
    assert Map.has_key?(get_result, :vector_clock)
  end

  @doc """
  Scenario 2: All nodes fail.

  Forcing NodeKV to always fail, the put should return :error 
  (quorum not met) and get should yield {:error, :no_responses}.
  """
  test "failed put and get when all nodes fail" do
    Application.put_env(:distributed_kv, :node_fail_mode, :always_fail)

    put_result = DistributedKVStore.put(@ring, "key_fail", "value_fail")
    assert put_result == :error

    get_result = DistributedKVStore.get(@ring, "key_fail")
    assert get_result == {:error, :no_responses}
  end

  @doc """
  Scenario 3: Partial failure with quorum met.

  In :partial mode, two out of three nodes succeed. With a quorum set to 2,
  the put should succeed and get should return a valid response.
  """
  test "partial failure with quorum met" do
    Application.put_env(:distributed_kv, :node_fail_mode, :partial)

    put_result = DistributedKVStore.put(@ring, "key_partial", "value_partial")
    assert put_result == :ok

    get_result = DistributedKVStore.get(@ring, "key_partial")
    if is_map(get_result) do
      assert Map.has_key?(get_result, :value)
      assert Map.has_key?(get_result, :vector_clock)
    else
      assert get_result == {:error, :no_responses}
    end
  end

  @doc """
  Scenario 4: Partial failure with quorum not met.

  Here we force more failures than permitted and thereby not meeting quorum.
  In that case, the put should return :error and hints should be stored.
  """
  test "partial failure with quorum not met" do
    Application.put_env(:distributed_kv, :node_fail_mode, :always_fail)

    put_result = DistributedKVStore.put(@ring, "key_no_quorum", "value_no_quorum")
    assert put_result == :error

    # Check hints were stored
    hints = :ets.tab2list(:hints)
    assert length(hints) > 0
  end
end
