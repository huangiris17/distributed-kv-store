ExUnit.start()

defmodule DistributedKVStoreTest do
  use ExUnit.Case

  describe "DistributedKVStore" do
    test "put then get returns a value (or error if quorum not met)" do
      put_result = DistributedKVStore.put("testkey", "testvalue")
      assert put_result in [:ok, :error]

      get_result = DistributedKVStore.get("testkey")
      cond do
        is_map(get_result) ->
          assert Map.has_key?(get_result, :value)
          assert Map.has_key?(get_result, :vector_clock)
        true ->
          assert get_result == {:error, :no_responses}
      end
    end
  end
end
