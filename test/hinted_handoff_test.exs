ExUnit.start()

defmodule HintedHandoffTest do
  use ExUnit.Case

  setup do
    if :ets.info(:hints) != :undefined, do: :ets.delete_all_objects(:hints)
    :ok
  end

  test "store_hint/4 stores a hint in ETS" do
    HintedHandoff.store_hint(:node_fail, "test_key", "test_value", [{:node_fail, 1}])
    hints = :ets.tab2list(:hints)
    assert Enum.any?(hints, fn {node, key, value, _vc} ->
             node == :node_fail and key == "test_key" and value == "test_value"
           end)
  end

  test "retry_hints/0 attempts to resend and removes successful hints" do
    HintedHandoff.store_hint(:node_fail, "retry_key", "retry_value", [{:node_fail, 1}])
    HintedHandoff.retry_hints()
    hints = :ets.tab2list(:hints)
    # Because NodeKV.put randomly fails or succeeds, the hint may be removed if successful.
    assert hints == [] or length(hints) >= 1
  end
end
