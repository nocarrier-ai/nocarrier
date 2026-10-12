defmodule NoCarrier.Players.Service.Nats do
  @behaviour NoCarrier.Players.Service

  alias NoCarrier.Nats
  alias NoCarrier.Players.OAuthID

  @bucket "player-status"
  @create_subject "cmd.player.create"
  @error_code_header "nats-service-error-code"
  @error_header "nats-service-error"

  @impl true
  def lookup_player(%OAuthID{} = oauth_id) do
    with {:ok, %{"player_id" => player_id}} <- read(OAuthID.key(oauth_id)),
         {:ok, entry} <- read(player_id) do
      decode_entry(entry)
    else
      {:ok, _other} -> {:error, :malformed_entry}
      other -> other
    end
  end

  @impl true
  def create_player(command) do
    body =
      Jason.encode!(%{
        player_id: command.player_id,
        oauth_id: %{provider: command.oauth_id.provider, uid: command.oauth_id.uid},
        display_name: command.display_name,
        dial_in_code: command.dial_in_code
      })

    case Nats.request(@create_subject, body) do
      {:ok, reply} -> decode_reply(reply)
      {:error, :timeout} -> {:error, :unavailable}
      {:error, :no_responders} -> {:error, :unavailable}
      {:error, _reason} = error -> error
    end
  end

  def decode_reply(%{body: body} = reply) do
    headers = Map.get(reply, :headers) || []

    case header(headers, @error_code_header) do
      nil -> decode_created(body)
      "400" -> {:error, {:invalid, header(headers, @error_header) || "rejected"}}
      "409" -> {:error, :already_registered}
      code -> {:error, {:rejected, code, header(headers, @error_header)}}
    end
  end

  defp decode_created(body) do
    with {:ok, reply} <- Jason.decode(body), {:ok, entry} <- decode_entry(reply) do
      {:ok, entry}
    else
      _error -> {:error, :malformed_reply}
    end
  end

  defp decode_entry(%{
         "player_id" => player_id,
         "display_name" => display_name,
         "dial_in_code" => dial_in_code
       })
       when is_binary(player_id) and is_binary(display_name) and is_binary(dial_in_code) do
    {:ok, %{player_id: player_id, display_name: display_name, dial_in_code: dial_in_code}}
  end

  defp decode_entry(_entry), do: {:error, :malformed_entry}

  defp read(key) do
    with {:ok, value} <- Nats.kv_get(@bucket, key),
         {:ok, map} when is_map(map) <- Jason.decode(value) do
      {:ok, map}
    else
      :not_found -> :not_found
      {:ok, _other} -> {:error, :malformed_entry}
      {:error, %Jason.DecodeError{}} -> {:error, :malformed_entry}
      {:error, reason} -> {:error, reason}
    end
  end

  defp header(headers, name) do
    Enum.find_value(headers, fn
      {key, value} -> if String.downcase(key) == name, do: value
      _other -> nil
    end)
  end
end
