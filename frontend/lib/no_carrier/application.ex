defmodule NoCarrier.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    topologies = Application.get_env(:libcluster, :topologies, [])

    children = [
      NoCarrierWeb.Telemetry,
      {Cluster.Supervisor, [topologies, [name: NoCarrier.ClusterSupervisor]]},
      {Phoenix.PubSub, name: NoCarrier.PubSub},
      {Horde.Registry, name: NoCarrier.HordeRegistry, keys: :unique, members: :auto},
      {Horde.DynamicSupervisor,
       name: NoCarrier.HordeSupervisor, strategy: :one_for_one, members: :auto},
      NoCarrierWeb.Endpoint
    ]

    opts = [strategy: :one_for_one, name: NoCarrier.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @impl true
  def config_change(changed, _new, removed) do
    NoCarrierWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
