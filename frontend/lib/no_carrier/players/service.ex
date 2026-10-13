defmodule NoCarrier.Players.Service do
  alias NoCarrier.Players.OAuthID

  @type entry :: %{player_id: String.t(), display_name: String.t(), dial_in_code: String.t()}

  @type command :: %{
          player_id: String.t(),
          oauth_id: OAuthID.t(),
          display_name: String.t(),
          dial_in_code: String.t()
        }

  @callback lookup_player(OAuthID.t()) :: {:ok, entry()} | :not_found | {:error, term()}
  @callback lookup_player_by_dial_in_code(String.t()) ::
              {:ok, entry()} | :not_found | {:error, term()}
  @callback create_player(command()) :: {:ok, entry()} | {:error, term()}
end
