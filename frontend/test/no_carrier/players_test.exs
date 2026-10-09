defmodule NoCarrier.PlayersTest do
  use ExUnit.Case, async: true

  import NoCarrier.PlayersFixtures

  alias NoCarrier.Players
  alias NoCarrier.Players.Player

  test "builds a player from a GitHub auth" do
    assert %Player{
             id: "github:42",
             provider: :github,
             uid: "42",
             login: "ada",
             name: "Ada Lovelace",
             email: "ada@example.com",
             avatar_url: "https://avatars.githubusercontent.com/u/42"
           } = Players.player_from_auth(github_auth_fixture())
  end
end
