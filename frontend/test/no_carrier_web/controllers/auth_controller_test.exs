defmodule NoCarrierWeb.AuthControllerTest do
  use NoCarrierWeb.ConnCase

  alias NoCarrier.Players.Player
  alias NoCarrierWeb.AuthController

  describe "GET /auth/:provider" do
    test "sends the player to GitHub", %{conn: conn} do
      conn = get(conn, ~p"/auth/github")

      assert redirected_to(conn) =~ "https://github.com/login/oauth/authorize"
    end

    test "rejects an unknown provider", %{conn: conn} do
      conn = get(conn, ~p"/auth/myspace")

      assert redirected_to(conn) == ~p"/"
      assert Phoenix.Flash.get(conn.assigns.flash, :error) =~ "Unknown sign-in provider"
    end
  end

  describe "GET /auth/github/callback" do
    test "logs the player in and sends them to the game", %{conn: conn} do
      conn = complete_github_callback(conn)

      assert redirected_to(conn) == ~p"/play"
      assert %Player{id: "github:42", login: "ada"} = get_session(conn, :player)
      assert Phoenix.Flash.get(conn.assigns.flash, :info) =~ "ada"
    end

    test "returns the player to the page they were refused", %{conn: conn} do
      conn = get(conn, ~p"/play")
      assert redirected_to(conn) == ~p"/"
      assert get_session(conn, :player_return_to) == ~p"/play"

      conn = conn |> recycle() |> complete_github_callback()

      assert redirected_to(conn) == ~p"/play"
      refute get_session(conn, :player_return_to)
    end

    test "reports a failed sign-in", %{conn: conn} do
      conn = get(conn, ~p"/auth/github/callback")

      assert redirected_to(conn) == ~p"/"
      assert Phoenix.Flash.get(conn.assigns.flash, :error) =~ "Sign-in failed"
      refute get_session(conn, :player)
    end
  end

  describe "DELETE /log-out" do
    test "clears the session", %{conn: conn} do
      conn = conn |> log_in_player(player_fixture()) |> delete(~p"/log-out")

      assert redirected_to(conn) == ~p"/"
      refute get_session(conn, :player)
    end

    test "works when nobody is logged in", %{conn: conn} do
      conn = delete(conn, ~p"/log-out")

      assert redirected_to(conn) == ~p"/"
    end
  end

  defp complete_github_callback(conn) do
    conn
    |> bypass_through(NoCarrierWeb.Router, [:browser])
    |> get(~p"/auth/github/callback")
    |> assign(:ueberauth_auth, github_auth_fixture())
    |> AuthController.callback(%{})
  end
end
