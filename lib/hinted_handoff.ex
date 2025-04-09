##################################
# Hinted Handoff Implementation  #
##################################

defmodule HintedHandoff do
    @moduledoc """
    Implements hinted handoff logic
    
    Hints are stored in an ETS table (:hints) so that writes failing on remote nodes
    can be retried later
    """
  
    # Initialize the ETS table for hints if it doesn't already exist
    def init do
      case :ets.info(:hints) do
        :undefined -> :ets.new(:hints, [:named_table, :public, :set])
        _ -> :ok
      end
    end
  
    # Public API to store a hint for a failed node
    def store_hint(failed_node, key, value, vector_clock) do
      init()
      :ets.insert(:hints, {failed_node, key, value, vector_clock})
    end
  
    # Public API to retry sending the stored hints
    def retry_hints do
      init()
      for {node, key, value, vector_clock} <- :ets.tab2list(:hints) do
        case NodeKV.put(key, value, vector_clock, node) do
          {:ok, _} -> :ets.delete(:hints, node)
          _ -> :ok
        end
      end
    end
end