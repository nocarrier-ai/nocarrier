defmodule NoCarrier.PlayersFixtures do
  alias NoCarrier.Players.Player

  def player_fixture(attrs \\ %{}) do
    struct!(
      %Player{
        id: "github:42",
        provider: :github,
        uid: "42",
        login: "ada",
        name: "Ada Lovelace",
        email: "ada@example.com",
        avatar_url: "https://avatars.githubusercontent.com/u/42"
      },
      attrs
    )
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
