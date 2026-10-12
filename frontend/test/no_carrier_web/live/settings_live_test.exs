defmodule NoCarrierWeb.SettingsLiveTest do
  use NoCarrierWeb.ConnCase

  import Phoenix.LiveViewTest

  test "refuses players who are not logged in", %{conn: conn} do
    assert {:error, {:redirect, %{to: "/", flash: flash}}} = live(conn, ~p"/settings")
    assert flash["error"] =~ "Sign in"
  end

  test "shows the player their dial-in code with a copy button", %{conn: conn} do
    player = player_fixture(dial_in_code: "1234-5678-9012")
    {:ok, view, _html} = conn |> log_in_player(player) |> live(~p"/settings")

    assert has_element?(view, "#settings")
    assert has_element?(view, "#dial-in-code")
    assert has_element?(view, "#dial-in-code-value", "1234-5678-9012")
    assert has_element?(view, "#dial-in-code", "telnet")
    assert has_element?(view, ~s(#copy-dial-in-code[data-copy="1234-5678-9012"]))
  end

  test "logs out a session stored before dial-in codes existed", %{conn: conn} do
    stored = player_fixture() |> Map.delete(:dial_in_code)
    conn = conn |> log_in_player(stored) |> get(~p"/settings")

    assert redirected_to(conn) == ~p"/"
    refute get_session(conn, :player)
  end
end
