defmodule DistributedKVStore.VectorClock do
    @moduledoc """
    An implementation of vector clocks.

    A vector clock is represented as a map where keys are node IDs and values
    are non-negative integers representing the number of events
    observed for that node. For example:

        %{"node1" => 3, "node2" => 5}

    This means that node1 has ticked 3 times and node2 5 times.
    """

    @type t :: %{optional(term()) => non_neg_integer()}

    @spec update(t() | nil, term()) :: t()
    def update(nil, node), do: %{node => 1}
    def update(clock, node) do
      Map.update(clock, node, 1, fn val -> val + 1 end)
    end

    @spec merge(t(), t()) :: t()
    def merge(clock1, clock2) do
      Map.merge(clock1, clock2, fn _node, count1, count2 ->
        max(count1, count2)
      end)
    end

    @spec compare(t(), t()) :: :equal | :descendant | :ancestor | :concurrent
    def compare(clock1, clock2) do
      keys = Map.keys(clock1) ++ Map.keys(clock2) |> Enum.uniq()

      {status1, status2} =
        Enum.reduce(keys, {true, true}, fn key, {is_greater, is_lesser} ->
          v1 = Map.get(clock1, key, 0)
          v2 = Map.get(clock2, key, 0)
          {is_greater && v1 >= v2, is_lesser && v1 <= v2}
        end)

      cond do
        status1 and status2 ->
          :equal
        status1 ->
          :descendant
        status2 ->
          :ancestor
        true ->
          :concurrent
      end
    end
  end
