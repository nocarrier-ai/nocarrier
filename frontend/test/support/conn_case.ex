defmodule NoCarrierWeb.ConnCase do
  use ExUnit.CaseTemplate

  using do
    quote do
      @endpoint NoCarrierWeb.Endpoint

      use NoCarrierWeb, :verified_routes

      import Plug.Conn
      import Phoenix.ConnTest
      import NoCarrierWeb.ConnCase
      import NoCarrier.PlayersFixtures
    end
  end

  setup _tags do
    {:ok, conn: Phoenix.ConnTest.build_conn()}
  end

  def start_players_service(_context) do
    start_supervised!(NoCarrier.Players.Service.Memory)
    :ok
  end

  def log_in_player(conn, player) do
    conn
    |> Phoenix.ConnTest.init_test_session(%{})
    |> Plug.Conn.put_session(:player, player)
  end

  def with_pending_account(conn, account) do
    conn
    |> Phoenix.ConnTest.init_test_session(%{})
    |> Plug.Conn.put_session(:pending_account, account)
  end
end
