defmodule NoCarrierWeb.PageControllerTest do
  use NoCarrierWeb.ConnCase

  test "GET / offers GitHub sign-in when logged out", %{conn: conn} do
    html = conn |> get(~p"/") |> html_response(200)

    assert html =~ ~s(id="sign-in")
    assert html =~ ~s(href="/auth/github")
    refute html =~ ~s(id="enter-game")
  end

  test "GET / offers the game when logged in", %{conn: conn} do
    html = conn |> log_in_player(player_fixture()) |> get(~p"/") |> html_response(200)

    assert html =~ ~s(id="enter-game")
    assert html =~ "ada"
    refute html =~ ~s(id="sign-in")
  end
end
