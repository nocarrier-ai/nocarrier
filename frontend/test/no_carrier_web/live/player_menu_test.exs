defmodule NoCarrierWeb.PlayerMenuTest do
  use NoCarrierWeb.ConnCase

  import Phoenix.LiveViewTest

  test "the player menu offers settings and log out", %{conn: conn} do
    {:ok, view, _html} = conn |> log_in_player(player_fixture()) |> live(~p"/play")

    assert has_element?(view, "#player-menu-trigger", "Ada")
    assert has_element?(view, ~s(#player-menu #settings-link[href="/settings"]))
    assert has_element?(view, ~s(#player-menu #log-out-link[data-method="delete"]))
  end

  test "settings is reachable from the player menu", %{conn: conn} do
    conn = log_in_player(conn, player_fixture())
    {:ok, view, _html} = live(conn, ~p"/play")

    {:ok, settings, _html} =
      view
      |> element("#player-menu #settings-link")
      |> render_click()
      |> follow_redirect(conn, ~p"/settings")

    assert has_element?(settings, "#dial-in-code")
  end

  test "the landing page shows the player menu when logged in", %{conn: conn} do
    html = conn |> log_in_player(player_fixture()) |> get(~p"/") |> html_response(200)

    assert html =~ ~s(id="player-menu")
    assert html =~ ~s(id="settings-link")
    assert html =~ ~s(id="log-out-link")
  end
end
