defmodule NoCarrierWeb.PlayerControllerTest do
  use NoCarrierWeb.ConnCase

  setup :start_players_service

  alias NoCarrier.Players.Player
  alias NoCarrier.Players.Service.Memory

  describe "GET /players/new" do
    test "refuses visitors who have not signed in", %{conn: conn} do
      conn = get(conn, ~p"/players/new")

      assert redirected_to(conn) == ~p"/"
      assert Phoenix.Flash.get(conn.assigns.flash, :error) =~ "Sign in"
    end

    test "offers the GitHub login as the default display name", %{conn: conn} do
      html =
        conn
        |> with_pending_account(account_fixture())
        |> get(~p"/players/new")
        |> html_response(200)

      assert html =~ ~s(id="player-form")
      assert html =~ ~s(value="ada")
      assert html =~ ~s(id="create-player")
    end
  end

  describe "POST /players" do
    test "creates the player and logs them in", %{conn: conn} do
      conn =
        conn
        |> with_pending_account(account_fixture())
        |> post(~p"/players", player: %{display_name: "Admiral Ada"})

      assert redirected_to(conn) == ~p"/play"

      assert %Player{display_name: "Admiral Ada", dial_in_code: dial_in_code, login: "ada"} =
               get_session(conn, :player)

      assert dial_in_code =~ ~r/^\d{4}-\d{4}-\d{4}$/
      refute get_session(conn, :pending_account)
      assert Phoenix.Flash.get(conn.assigns.flash, :info) =~ "Admiral Ada"
      assert [%{display_name: "Admiral Ada"}] = Memory.players()
    end

    test "returns the player to the page they were refused", %{conn: conn} do
      conn =
        conn
        |> with_pending_account(account_fixture())
        |> put_session(:player_return_to, ~p"/settings")
        |> post(~p"/players", player: %{display_name: "Admiral Ada"})

      assert redirected_to(conn) == ~p"/settings"
    end

    test "shows what is wrong with a display name", %{conn: conn} do
      html =
        conn
        |> with_pending_account(account_fixture())
        |> post(~p"/players", player: %{display_name: "   "})
        |> html_response(200)

      assert html =~ ~s(id="player-form")
      assert html =~ "Pick a display name"
      assert Memory.players() == []
    end

    test "signs in the existing player when the oauth ID was registered meanwhile", %{conn: conn} do
      register_player()

      conn =
        conn
        |> with_pending_account(account_fixture())
        |> post(~p"/players", player: %{display_name: "Someone else"})

      assert redirected_to(conn) == ~p"/play"
      assert %Player{id: "p-ada", display_name: "Ada"} = get_session(conn, :player)
    end

    test "keeps the form when the universe is unreachable", %{conn: conn} do
      Memory.fail_with(:unavailable)

      conn =
        conn
        |> with_pending_account(account_fixture())
        |> post(~p"/players", player: %{display_name: "Admiral Ada"})

      assert html_response(conn, 200) =~ ~s(value="Admiral Ada")
      assert Phoenix.Flash.get(conn.assigns.flash, :error) =~ "unreachable"
      refute get_session(conn, :player)
    end

    test "refuses a post without a signed-in account", %{conn: conn} do
      conn = post(conn, ~p"/players", player: %{display_name: "Admiral Ada"})

      assert redirected_to(conn) == ~p"/"
      assert Memory.players() == []
    end
  end
end
