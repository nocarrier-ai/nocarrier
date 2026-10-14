defmodule NoCarrier.News do
  @pubsub NoCarrier.PubSub

  def subscribe(player_id), do: Phoenix.PubSub.subscribe(@pubsub, topic(player_id))

  def broadcast(player_id, %{tick: tick, text: text}) when is_integer(tick) and is_binary(text) do
    Phoenix.PubSub.local_broadcast(@pubsub, topic(player_id), {:news, %{tick: tick, text: text}})
  end

  defp topic(player_id), do: "news:" <> player_id
end
