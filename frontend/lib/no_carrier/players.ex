defmodule NoCarrier.Players do
  alias NoCarrier.Players.Account
  alias NoCarrier.Players.OAuthID
  alias NoCarrier.Players.Player

  @display_name_limit 24
  @player_id_bytes 9
  @dial_in_code_digits 12
  @dial_in_code_group 4

  def account_from_auth(%Ueberauth.Auth{} = auth) do
    %Account{
      oauth_id: %OAuthID{provider: to_string(auth.provider), uid: to_string(auth.uid)},
      login: auth.info.nickname,
      name: auth.info.name,
      email: auth.info.email,
      avatar_url: auth.info.image
    }
  end

  def find_player(%Account{} = account) do
    case service().lookup_player(account.oauth_id) do
      {:ok, entry} -> {:ok, player(entry, account)}
      :not_found -> :not_found
      {:error, reason} -> {:error, reason}
    end
  end

  def create_player(%Account{} = account, display_name) do
    with {:ok, display_name} <- validate_display_name(display_name) do
      command = %{
        player_id: generate_player_id(),
        oauth_id: account.oauth_id,
        display_name: display_name,
        dial_in_code: generate_dial_in_code()
      }

      case service().create_player(command) do
        {:ok, entry} ->
          {:ok, player(entry, account)}

        {:error, :already_registered} ->
          with :not_found <- find_player(account), do: {:error, :already_registered}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  def validate_display_name(display_name) when is_binary(display_name) do
    display_name = String.trim(display_name)
    length = String.length(display_name)

    cond do
      display_name == "" ->
        {:error, {:invalid, "Pick a display name."}}

      length > @display_name_limit ->
        {:error,
         {:invalid, "Display name is #{length} characters, the limit is #{@display_name_limit}."}}

      Regex.match?(~r/[[:cntrl:]]/u, display_name) ->
        {:error, {:invalid, "Display name contains a control character."}}

      true ->
        {:ok, display_name}
    end
  end

  def validate_display_name(_display_name), do: {:error, {:invalid, "Pick a display name."}}

  def display_name_limit, do: @display_name_limit

  def generate_player_id do
    @player_id_bytes |> :crypto.strong_rand_bytes() |> Base.url_encode64(padding: false)
  end

  def generate_dial_in_code do
    digits =
      :crypto.strong_rand_bytes(8)
      |> :binary.decode_unsigned()
      |> rem(Integer.pow(10, @dial_in_code_digits))
      |> Integer.to_string()
      |> String.pad_leading(@dial_in_code_digits, "0")

    for(<<group::binary-size(@dial_in_code_group) <- digits>>, do: group)
    |> Enum.join("-")
  end

  def player_from_session(%{__struct__: Player} = stored) do
    player = struct(Player, Map.from_struct(stored))

    if Enum.all?([player.id, player.display_name, player.dial_in_code], &is_binary/1) do
      player
    end
  end

  def player_from_session(_stored), do: nil

  defp player(entry, %Account{} = account) do
    %Player{
      id: entry.player_id,
      display_name: entry.display_name,
      dial_in_code: entry.dial_in_code,
      oauth_id: account.oauth_id,
      login: account.login,
      name: account.name,
      email: account.email,
      avatar_url: account.avatar_url
    }
  end

  defp service, do: Application.fetch_env!(:no_carrier, :players_service)
end
