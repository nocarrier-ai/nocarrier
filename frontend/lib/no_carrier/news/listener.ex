defmodule NoCarrier.News.Listener do
  require Logger

  alias NoCarrier.News

  def child_spec(_opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, []}}
  end

  def start_link do
    if Application.get_env(:no_carrier, NoCarrier.Nats) do
      Gnat.ConsumerSupervisor.start_link(
        %{
          connection_name: NoCarrier.Nats.Connection,
          consuming_function: {__MODULE__, :handle_message},
          subscription_topics: [%{topic: "news.>"}]
        },
        name: __MODULE__
      )
    else
      :ignore
    end
  end

  def handle_message(%{topic: "news." <> player_id, body: body}) do
    case Jason.decode(body) do
      {:ok, %{"tick" => tick, "text" => text}} when is_integer(tick) and is_binary(text) ->
        News.broadcast(player_id, %{tick: tick, text: text})

      _other ->
        Logger.warning("malformed news item", player_id: player_id)
        :ok
    end
  end

  def handle_message(_message), do: :ok
end
