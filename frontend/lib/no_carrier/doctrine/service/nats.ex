defmodule NoCarrier.Doctrine.Service.Nats do
  @behaviour NoCarrier.Doctrine.Service

  alias NoCarrier.Nats

  @bucket "doctrine"
  @update_subject "cmd.doctrine.update"
  @error_code_header "nats-service-error-code"
  @error_header "nats-service-error"

  @impl true
  def doctrine(avatar_id) do
    with {:ok, value} <- Nats.kv_get(@bucket, avatar_id),
         {:ok, map} when is_map(map) <- Jason.decode(value) do
      decode_entry(map)
    else
      :not_found -> :not_found
      {:ok, _other} -> {:error, :malformed_entry}
      {:error, %Jason.DecodeError{}} -> {:error, :malformed_entry}
      {:error, _reason} = error -> error
    end
  end

  @impl true
  def update(avatar_id, orders, hooks) do
    body = Jason.encode!(%{avatar_id: avatar_id, orders: orders, hooks: hooks})

    case Nats.request(@update_subject, body) do
      {:ok, reply} -> decode_reply(reply)
      {:error, :timeout} -> {:error, :unavailable}
      {:error, :no_responders} -> {:error, :unavailable}
      {:error, _reason} = error -> error
    end
  end

  def decode_reply(%{body: body} = reply) do
    headers = Map.get(reply, :headers) || []

    case header(headers, @error_code_header) do
      nil -> decode_receipt(body)
      "400" -> {:error, {:invalid, header(headers, @error_header) || "rejected"}}
      "409" -> {:error, :conflict}
      code -> {:error, {:rejected, code, header(headers, @error_header)}}
    end
  end

  defp decode_receipt(body) do
    case Jason.decode(body) do
      {:ok, %{"revision" => revision, "tick" => tick, "hash" => hash}}
      when is_integer(revision) and is_integer(tick) and is_binary(hash) ->
        {:ok, %{revision: revision, tick: tick, hash: hash}}

      _other ->
        {:error, :malformed_reply}
    end
  end

  defp decode_entry(%{"revision" => revision, "tick" => tick, "orders" => orders} = map)
       when is_integer(revision) and is_integer(tick) and is_list(orders) do
    hooks = Map.get(map, "hooks") || %{}

    if Enum.all?(orders, &is_binary/1) and valid_hooks?(hooks) do
      {:ok, %{revision: revision, tick: tick, orders: orders, hooks: hooks}}
    else
      {:error, :malformed_entry}
    end
  end

  defp decode_entry(_map), do: {:error, :malformed_entry}

  defp valid_hooks?(hooks) when is_map(hooks) do
    Enum.all?(hooks, fn {hook, order} -> is_binary(hook) and is_binary(order) end)
  end

  defp valid_hooks?(_hooks), do: false

  defp header(headers, name) do
    Enum.find_value(headers, fn
      {key, value} -> if String.downcase(key) == name, do: value
      _other -> nil
    end)
  end
end
