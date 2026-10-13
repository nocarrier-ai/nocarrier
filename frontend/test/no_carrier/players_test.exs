defmodule NoCarrier.PlayersTest do
  use ExUnit.Case, async: false

  import NoCarrier.PlayersFixtures

  alias NoCarrier.Players
  alias NoCarrier.Players.Account
  alias NoCarrier.Players.OAuthID
  alias NoCarrier.Players.Player
  alias NoCarrier.Players.Service.Memory

  @dial_in_code_format ~r/^\d{4}-\d{4}-\d{4}$/
  @player_id_format ~r/^[A-Za-z0-9_-]{12}$/

  setup do
    start_supervised!(Memory)
    :ok
  end

  test "reads the account out of a GitHub auth" do
    assert %Account{
             oauth_id: %OAuthID{provider: "github", uid: "42"},
             login: "ada",
             name: "Ada Lovelace",
             email: "ada@example.com",
             avatar_url: "https://avatars.githubusercontent.com/u/42"
           } = Players.account_from_auth(github_auth_fixture())
  end

  test "finds nobody for an oauth ID the universe has never seen" do
    assert Players.find_player(account_fixture()) == :not_found
  end

  test "finds a registered player and fills in the account" do
    register_player()

    assert {:ok,
            %Player{
              id: "p-ada",
              display_name: "Ada",
              dial_in_code: "7719-0402-5518",
              oauth_id: %OAuthID{provider: "github", uid: "42"},
              login: "ada",
              avatar_url: "https://avatars.githubusercontent.com/u/42"
            }} = Players.find_player(account_fixture())
  end

  test "finds a player by dial-in code" do
    register_player()

    assert {:ok, %{player_id: "p-ada", display_name: "Ada"}} = Players.dial_in("7719-0402-5518")
    assert Players.dial_in("0000-0000-0000") == :not_found
  end

  test "refuses a malformed dial-in code without asking the universe" do
    Memory.fail_with(:unavailable)

    assert Players.dial_in("771904025518") == :not_found
    assert Players.dial_in("7719-0402-551") == :not_found
  end

  test "reports an unreachable universe" do
    Memory.fail_with(:unavailable)

    assert Players.find_player(account_fixture()) == {:error, :unavailable}
  end

  test "creates a player with a generated ID and dial-in code" do
    assert {:ok, %Player{} = player} =
             Players.create_player(account_fixture(), "  Admiral Ada  ")

    assert player.display_name == "Admiral Ada"
    assert player.id =~ @player_id_format
    assert player.dial_in_code =~ @dial_in_code_format
    assert player.login == "ada"
    assert [%{player_id: id}] = Memory.players()
    assert id == player.id
  end

  test "signs in the existing player when the oauth ID is already registered" do
    existing = register_player()

    assert {:ok, %Player{id: "p-ada", display_name: "Ada"}} =
             Players.create_player(account_fixture(), "Someone else")

    assert [%{player_id: id}] = Memory.players()
    assert id == existing.id
  end

  test "rejects a bad display name without asking the universe" do
    assert {:error, {:invalid, message}} = Players.create_player(account_fixture(), "   ")
    assert message =~ "Pick a display name"

    assert {:error, {:invalid, message}} =
             Players.create_player(account_fixture(), String.duplicate("a", 25))

    assert message =~ "limit is 24"

    assert {:error, {:invalid, message}} =
             Players.create_player(account_fixture(), "ada\nlovelace")

    assert message =~ "control character"

    assert Memory.players() == []
  end

  test "generates dial-in codes as three groups of four digits" do
    assert Players.generate_dial_in_code() =~ @dial_in_code_format
  end

  test "generates fresh IDs and codes each time" do
    codes = for _ <- 1..20, do: Players.generate_dial_in_code()
    ids = for _ <- 1..20, do: Players.generate_player_id()

    assert length(Enum.uniq(codes)) == 20
    assert length(Enum.uniq(ids)) == 20
  end

  test "restores a complete stored player as is" do
    player = player_fixture()

    assert Players.player_from_session(player) == player
  end

  test "drops a player stored before display names and dial-in codes existed" do
    stored = player_fixture() |> Map.delete(:display_name) |> Map.delete(:dial_in_code)

    assert Players.player_from_session(stored) == nil
  end

  test "restores nothing when nobody is stored" do
    assert Players.player_from_session(nil) == nil
  end
end
