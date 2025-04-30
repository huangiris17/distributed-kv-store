defmodule GossipTest do
  use ExUnit.Case

  @gossip_interval   1_000   # must match @gossip_interval in Gossip
  @gossip_rounds       3     # number of rounds to wait
  @failure_threshold 3_000   # must match @failure_threshold in Gossip
  @gossip_timeout   @gossip_interval + 500  # give it 1.5s to catch the receive window

  setup do
    nodes = [:node1, :node2, :node3]

    now = System.system_time(:millisecond)

    initial_view =
      for n <- nodes, into: %{} do
        {n, %{status: :alive, timestamp: now}}
      end

    Enum.each(nodes, fn node ->
      unless Process.whereis(:"gossip_#{node}") do
        spawn(fn -> DistributedKVStore.Gossip.start(node, initial_view) end)
      end
    end)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        if pid = Process.whereis(:"gossip_#{node}"), do: Process.exit(pid, :kill)
      end)
    end)

    Process.sleep(@gossip_rounds * @gossip_interval)
    :ok
  end

  test "membership view includes all nodes and marks them as alive" do
    pid = Process.whereis(:gossip_node1)
    send(pid, {:get_view, self()})

    view =
      receive do
        {:view_response, v} -> v
      after
        @gossip_timeout -> flunk("No view response from gossip_node1")
      end

    assert Map.keys(view) |> Enum.sort() == [:node1, :node2, :node3]
    for {node, %{status: status}} <- view do
      assert status == :alive, "Expected #{node} alive, got #{status}"
    end
  end

  test "outdated node record is marked as failed" do
    pid = Process.whereis(:gossip_node1)
    now = System.system_time(:millisecond)
    old_record = %{status: :alive, timestamp: now - (@failure_threshold + 1_000)}

    send(pid, {:gossip, :old_node, %{old_node: old_record}})

    Process.sleep(@failure_threshold + @gossip_interval)

    send(pid, {:get_view, self()})

    updated_view =
      receive do
        {:view_response, v} -> v
      after
        @gossip_timeout -> flunk("No view response after injecting outdated record")
      end

    assert updated_view[:old_node].status == :failed,
           "Expected :old_node to be failed, got #{inspect(updated_view[:old_node])}"
  end
end
