defmodule GossipTest do
    use ExUnit.Case
  
    @gossip_timeout 500   # milliseconds to wait for a view response
    @gossip_wait    2000  # time to wait to allow gossip rounds
  
    setup do
      # Start gossip processes for nodes :node1, :node2, and :node3.
      # We start them if they are not already registered.
      for node <- [:node1, :node2, :node3] do
        unless Process.whereis(:"gossip_#{node}") do
          spawn(fn -> DistributedKVStore.Gossip.start(node) end)
        end
      end
      # Allow some gossip rounds (each round is 1000ms in our module)
      Process.sleep(@gossip_wait)
      :ok
    end
  
    @doc """
    Test that the membership view from a gossip process includes all started nodes,
    and that their records are marked as :alive.
    """
    test "membership view includes all nodes and marks them as alive" do
      # Query node1's membership view.
      gossip_pid = Process.whereis(:"gossip_node1")
      send(gossip_pid, {:get_view, self()})
  
      view =
        receive do
          {:view_response, v} -> v
        after
          @gossip_timeout -> flunk("No view response from gossip_node1")
        end
  
      # Assert that the view includes node1, node2, and node3.
      assert Map.has_key?(view, :node1)
      assert Map.has_key?(view, :node2)
      assert Map.has_key?(view, :node3)
  
      # And that each node's status is :alive.
      for {node, record} <- view do
        assert record.status == :alive,
               "Expected #{inspect(node)} to be alive, got #{inspect(record.status)}"
      end
    end
  
    @doc """
    Test that a node record with an outdated timestamp is eventually marked as :failed.
  
    We simulate a stale record by sending a gossip message with a record for an "old_node"
    whose timestamp is far in the past. After waiting beyond the failure threshold,
    the view should mark that node as :failed.
    """
    test "outdated node record is marked as failed" do
      now = :os.system_time(:millisecond)
      # Create a fake record with a timestamp 10 seconds (10_000 ms) in the past.
      old_record = %{status: :alive, timestamp: now - 10_000}
  
      # Inject a gossip message for :old_node into gossip_node1.
      gossip_pid = Process.whereis(:"gossip_node1")
      send(gossip_pid, {:gossip, :old_node, %{old_node: old_record}})
  
      # Wait long enough for the failure detection to run.
      Process.sleep(2000)
  
      # Query the updated membership view.
      send(gossip_pid, {:get_view, self()})
      updated_view =
        receive do
          {:view_response, v} -> v
        after
          @gossip_timeout -> flunk("No view response after injecting outdated record")
        end
  
      # The outdated :old_node record should now be marked as :failed.
      assert updated_view[:old_node].status == :failed,
             "Expected :old_node to be marked as failed, got: #{inspect(updated_view[:old_node])}"
    end
  end
  