defmodule NoCarrier.Telnet do
  alias NoCarrier.Telnet.Session

  def child_spec(_opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, []}, type: :supervisor}
  end

  def start_link do
    config = Application.fetch_env!(:no_carrier, __MODULE__)

    if Keyword.get(config, :enabled, true) do
      ThousandIsland.start_link(
        port: Keyword.fetch!(config, :port),
        handler_module: Session,
        read_timeout: :infinity,
        supervisor_options: [name: __MODULE__]
      )
    else
      :ignore
    end
  end
end
