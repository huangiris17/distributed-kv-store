##################################
# Hinted Handoff Implementation  #
##################################

defmodule DistributedKVStore.HintedHandoff do
    @moduledoc """
    Implements hinted handoff logic

    Hints are stored in an ETS table (:hints) so that writes failing on remote nodes
    can be retried later
    """

    # Sub-module alias
    alias DistributedKVStore.NodeKV

    # Initialize the ETS table for hints if it doesn't already exist
    def init do
      case :ets.info(:hints) do
        :undefined -> :ets.new(:hints, [:named_table, :public, :set])
        _ -> :ok
      end
    end

    # Public API to store a hint for a failed node
    def store_hint(failed_node, key, value, vector_clock) do
      retry_count = 0
      :ets.insert(:hints, {failed_node, key, value, vector_clock, retry_count})
    end

    # Public API to retry sending the stored hints
    def retry_hints do
      for {node, key, value, vector_clock, retry_count} <- :ets.tab2list(:hints) do
        if retry_count < 5 do
          timestamp = System.system_time(:millisecond)
          result = NodeKV.put(node, key, value, vector_clock, timestamp)
          case result do
            {:ok, _} -> :ets.delete_object(:hints, {node, key, value, vector_clock, retry_count})
            _ -> :ets.insert(:hints, {node, key, value, vector_clock, retry_count + 1})
          end
        else
          IO.puts("Retry limit reached for #{node} and key #{key}")
        end
      end
    end
end