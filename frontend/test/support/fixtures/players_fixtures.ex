defmodule NoCarrier.PlayersFixtures do
  alias NoCarrier.Players.Account
  alias NoCarrier.Players.OAuthID
  alias NoCarrier.Players.Player
  alias NoCarrier.Players.Service.Memory

  def oauth_id_fixture(attrs \\ %{}) do
    struct!(%OAuthID{provider: "github", uid: "42"}, attrs)
  end

  def account_fixture(attrs \\ %{}) do
    struct!(
      %Account{
        oauth_id: oauth_id_fixture(),
        login: "ada",
        name: "Ada Lovelace",
        email: "ada@example.com",
        avatar_url: "https://avatars.githubusercontent.com/u/42"
      },
      attrs
    )
  end

  def player_fixture(attrs \\ %{}) do
    struct!(
      %Player{
        id: "p-ada",
        display_name: "Ada",
        dial_in_code: "7719-0402-5518",
        oauth_id: oauth_id_fixture(),
        login: "ada",
        name: "Ada Lovelace",
        email: "ada@example.com",
        avatar_url: "https://avatars.githubusercontent.com/u/42"
      },
      attrs
    )
  end

  def register_player(player \\ player_fixture()) do
    {:ok, _entry} =
      Memory.create_player(%{
        player_id: player.id,
        oauth_id: player.oauth_id,
        display_name: player.display_name,
        dial_in_code: player.dial_in_code
      })

    player
  end

  def github_auth_fixture do
    %Ueberauth.Auth{
      provider: :github,
      strategy: Ueberauth.Strategy.Github,
      uid: 42,
      info: %Ueberauth.Auth.Info{
        nickname: "ada",
        name: "Ada Lovelace",
        email: "ada@example.com",
        image: "https://avatars.githubusercontent.com/u/42"
      },
      credentials: %Ueberauth.Auth.Credentials{},
      extra: %Ueberauth.Auth.Extra{}
    }
  end
end
