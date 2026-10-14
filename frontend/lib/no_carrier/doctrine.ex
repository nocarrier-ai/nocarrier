defmodule NoCarrier.Doctrine do
  @word_limit 400
  @hooks ["attacked", "moved"]

  def doctrine(avatar_id), do: service().doctrine(avatar_id)

  def update(avatar_id, orders, hooks), do: service().update(avatar_id, orders, hooks)

  def word_count(text) when is_binary(text), do: length(String.split(text))

  def word_count(orders, hooks) do
    (orders ++ Map.values(hooks))
    |> Enum.map(&length(String.split(&1)))
    |> Enum.sum()
  end

  def word_limit, do: @word_limit

  def hooks, do: @hooks

  defp service, do: Application.fetch_env!(:no_carrier, :doctrine_service)
end
