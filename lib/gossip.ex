defmodule DistributedKVStore.Gossip do
    @moduledoc """
    A fully implemented gossip protocol for decentralized membership dissemination.

    Each node maintains a membership view—a map where keys are node IDs and values
    are records with the node’s status and the last updated timestamp. For example:

        %{
          node1: %{status: :alive, timestamp: 1636000000000},
          node2: %{status: :alive, timestamp: 1636000000500},
          node3: %{status: :failed, timestamp: 1635999000000}
        }

    Every gossip round (every @gossip_interval milliseconds), a node:
      - Updates its own record with the current time.
      - Randomly selects one peer (from its view, excluding itself) and sends its view.
      - Processes any incoming gossip messages (merging membership views) for a brief timeout.
      - Marks nodes as failed if their records haven’t been updated within @failure_threshold milliseconds.
      - Optionally answers external requests to retrieve its current membership view.

    Each gossip process is registered using the name `:"gossip_{node_id}"` so that peers can locate it.
    """

    @gossip_interval 1_000    # Interval (ms) between gossip rounds
    @receive_timeout   100     # ms to wait for incoming gossip messages each round
    @failure_threshold 3_000   # ms without update before marking a node as failed

    # Sub-module alias
    alias DistributedKVStore.HintedHandoff

    @doc """
    Starts the gossip process for a given node.

    ## Parameters
      - node_id: An identifier (atom) for this node.
      - initial_view: (Optional) a membership view map; by default the view contains only self.

    The process registers itself under the name `:"gossip_{node_id}"` and begins the gossip loop.
    """
    def start(node_id, initial_view \\ %{}) do
      Process.register(self(), gossip_name(node_id))
      # Ensure our own record is in the view
      view = Map.put(initial_view, node_id, %{status: :alive, timestamp: current_time()})
      loop(node_id, view)
    end

    defp gossip_name(node_id), do: :"gossip_#{node_id}"

    # The main loop—update, send gossip, receive gossip, update failures, and sleep.
    defp loop(node_id, view) do
      # Update own membership record with current time.
      view = update_own(view, node_id)

      # Send our view to one random peer (if any exist).
      view
      |> Map.keys()
      |> Enum.filter(fn peer -> peer != node_id end)
      |> Enum.take_random(1)
      |> Enum.each(fn peer ->
        if (peer_pid = Process.whereis(gossip_name(peer))) do
          send(peer_pid, {:gossip, node_id, view})
        end
      end)

      # Process incoming gossip messages for the defined timeout.
      view = receive_messages(view, @receive_timeout)

      # (Optional) Process any external info requests. For this implementation,
      # all such messages are handled within receive_messages/2.

      # Mark nodes as failed if they haven't updated recently.
      view = mark_failed(view, current_time(), @failure_threshold)

      Process.sleep(@gossip_interval)
      loop(node_id, view)
    end

    # Recursively receive gossip (and info) messages until timeout expires.
    defp receive_messages(view, timeout) do
      receive do
        # Incoming gossip from a peer.
        {:gossip, _from, peer_view} ->
          merged = merge_views(view, peer_view)
          receive_messages(merged, timeout)
        # External request: return the current view to the requester.
        {:get_view, requester} ->
          send(requester, {:view_response, view})
          receive_messages(view, timeout)
      after
        timeout ->
          view
      end
    end

    # Merge two membership views.
    # For nodes present in both, keep the record with the latest timestamp.
    defp merge_views(view1, view2) do
      Map.merge(view1, view2, fn _node, data1, data2 ->
        if data1.timestamp >= data2.timestamp, do: data1, else: data2
      end)
    end

    # Update this node's own record in the membership view.
    defp update_own(view, node_id) do
      Map.update(view, node_id, %{status: :alive, timestamp: current_time()}, fn data ->
        %{data | timestamp: current_time(), status: :alive}
      end)
    end

    # Mark nodes as failed if their last update is older than the threshold.
    defp mark_failed(view, now, threshold) do
      view
      |> Enum.map(fn {node, data} ->
        new_data =
          if now - data.timestamp > threshold do
            %{data | status: :failed}
          else
            data
          end

          if new_data.status == :alive and data.status == :failed do
            IO.puts("Node #{node} has recovered, retrying hints.")
            HintedHandoff.retry_hints()
          end

        {node, new_data}
      end)
      |> Enum.into(%{})
    end

    defp current_time do
      :os.system_time(:millisecond)
    end
  end
