ExUnit.start()

defmodule HintedHandoffTest do
  use ExUnit.Case

  setup do
    # Make sure ETS table is fresh before every test.
    if :ets.info(:hints) != :undefined, do: :ets.delete_all_objects(:hints)
    :ok
  end

  @doc """
  Test that store_hint/4 correctly writes a hint record in ETS.
  """
  test "store_hint/4 stores a hint in ETS" do
    HintedHandoff.store_hint(:node_fail, "hint_key", "hint_value", [{:node_fail, 1}])
    hints = :ets.tab2list(:hints)
    assert Enum.any?(hints, fn {node, key, value, _vc} ->
             node == :node_fail and key == "hint_key" and value == "hint_value"
           end)
  end

  @doc """
  Test that retry_hints/0 removes a hint when the node recovers.
  
  First, we store a hint. Then, by forcing nodes to always succeed,
  we simulate node recovery and verify that the hint is removed.
  """
  test "retry_hints/0 removes hint when node recovers" do
    # Store a hint for a node (simulate failure).
    Application.put_env(:distributed_kv, :node_fail_mode, :always_fail)
    HintedHandoff.store_hint(:node_recover, "retry_key", "retry_value", [{:node_recover, 1}])
    assert Enum.any?(:ets.tab2list(:hints), fn {node, _, _, _} -> node == :node_recover end)

    # Now simulate recovery – forcing always succeed.
    Application.put_env(:distributed_kv, :node_fail_mode, :always_succeed)
    HintedHandoff.retry_hints()
    hints_after = :ets.tab2list(:hints)
    refute Enum.any?(hints_after, fn {node, key, value, _vc} ->
             node == :node_recover and key == "retry_key" and value == "retry_value"
           end)
  end

  @doc """
  Test that retry_hints/0 leaves the hint if the node continues to fail.
  
  We force a node to always fail and verify that after a retry
  the hint remains in ETS.
  """
  test "retry_hints/0 does not remove hint when node still fails" do
    Application.put_env(:distributed_kv, :node_fail_mode, :always_fail)
    HintedHandoff.store_hint(:node_still_fail, "persist_key", "persist_value", [{:node_still_fail, 1}])
    hints_before = :ets.tab2list(:hints)
    assert Enum.any?(hints_before, fn {node, _, _, _} -> node == :node_still_fail end)

    HintedHandoff.retry_hints()
    hints_after = :ets.tab2list(:hints)
    assert Enum.any?(hints_after, fn {node, key, value, _vc} ->
             node == :node_still_fail and key == "persist_key" and value == "persist_value"
           end)
  end
end
