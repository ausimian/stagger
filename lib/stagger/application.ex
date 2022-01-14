defmodule Stagger.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # A registry used to hold each MsgQueue, keyed by the path of the MsgQueue file
      %{id: Stagger.MsgQueueRegistry, start: {Registry, :start_link, [[keys: :unique, name: Stagger.MsgQueueRegistry]]}},
      # The supervisor for the MsgQueue processes
      {DynamicSupervisor, strategy: :one_for_one, name: Stagger.MsgQueueSupervisor},
    ]

    opts = [strategy: :one_for_one, name: Stagger.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
