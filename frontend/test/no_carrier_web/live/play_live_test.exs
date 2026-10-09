defmodule NoCarrierWeb.PlayLiveTest do
  use NoCarrierWeb.ConnCase

  import Phoenix.LiveViewTest

  test "refuses players who are not logged in", %{conn: conn} do
    assert {:error, {:redirect, %{to: "/", flash: flash}}} = live(conn, ~p"/play")
    assert flash["error"] =~ "Sign in"
  end

  test "shows the admiral their placeholder game", %{conn: conn} do
    {:ok, view, _html} = conn |> log_in_player(player_fixture()) |> live(~p"/play")

    assert has_element?(view, "#play")
    assert has_element?(view, "#play", "ada")
    assert has_element?(view, "#flagship")
    assert has_element?(view, "#doctrine")
    assert has_element?(view, "#node-status")
  end
end
