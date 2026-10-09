defmodule NoCarrierWeb.PlayerAuth do
  use NoCarrierWeb, :verified_routes

  import Plug.Conn
  import Phoenix.Controller

  alias NoCarrier.Players.Scope

  @login_required_message "Sign in to reach your admiral."

  def log_in_player(conn, player) do
    return_to = get_session(conn, :player_return_to)

    conn
    |> renew_session()
    |> put_session(:player, player)
    |> put_session(:live_socket_id, "players_sessions:#{player.id}")
    |> redirect(to: return_to || signed_in_path(conn))
  end

  def log_out_player(conn) do
    if live_socket_id = get_session(conn, :live_socket_id) do
      NoCarrierWeb.Endpoint.broadcast(live_socket_id, "disconnect", %{})
    end

    conn
    |> renew_session()
    |> redirect(to: ~p"/")
  end

  def fetch_current_scope_for_player(conn, _opts) do
    assign(conn, :current_scope, Scope.for_player(get_session(conn, :player)))
  end

  def on_mount(:mount_current_scope, _params, session, socket) do
    {:cont, mount_current_scope(socket, session)}
  end

  def on_mount(:ensure_authenticated, _params, session, socket) do
    socket = mount_current_scope(socket, session)

    if socket.assigns.current_scope do
      {:cont, socket}
    else
      socket =
        socket
        |> Phoenix.LiveView.put_flash(:error, @login_required_message)
        |> Phoenix.LiveView.redirect(to: ~p"/")

      {:halt, socket}
    end
  end

  def require_authenticated_player(conn, _opts) do
    if conn.assigns.current_scope do
      conn
    else
      conn
      |> put_flash(:error, @login_required_message)
      |> maybe_store_return_to()
      |> redirect(to: ~p"/")
      |> halt()
    end
  end

  def signed_in_path(_conn), do: ~p"/play"

  defp mount_current_scope(socket, session) do
    Phoenix.Component.assign_new(socket, :current_scope, fn ->
      Scope.for_player(session["player"])
    end)
  end

  defp renew_session(conn) do
    delete_csrf_token()

    conn
    |> configure_session(renew: true)
    |> clear_session()
  end

  defp maybe_store_return_to(%{method: "GET"} = conn) do
    put_session(conn, :player_return_to, current_path(conn))
  end

  defp maybe_store_return_to(conn), do: conn
end
