defmodule DistributedKVStore.Application do
    @moduledoc """
    OTP entry-point for the distributed-kv-store.
    """

    use Application

    def start(_type, _args) do
      topologies =
        Application.get_env(:libcluster, :topologies, [])

      cluster_child =
        {Cluster.Supervisor, [topologies, [name: DistributedKVStore.ClusterSupervisor]]}

      children = [
        {Registry, keys: :unique, name: DistributedKVStore.Registry},

        cluster_child,

        DistributedKVStore.NodeKV,
        DistributedKVStore.MerkleSync
      ]

      children =
        if topologies == [] do
          List.delete(children, cluster_child)
        else
          children
        end

      Supervisor.start_link(children,
        strategy: :one_for_one,
        name: DistributedKVStore.Supervisor
      )
    end
  end
