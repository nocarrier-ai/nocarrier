defmodule NoCarrierWeb.PlayerController do
  use NoCarrierWeb, :controller

  alias NoCarrier.Players
  alias NoCarrier.Players.Account
  alias NoCarrierWeb.PlayerAuth

  plug :require_pending_account

  def new(conn, _params) do
    render(conn, :new, form: player_form(%{"display_name" => conn.assigns.pending_account.login}))
  end

  def create(conn, %{"player" => %{"display_name" => display_name} = params}) do
    case Players.create_player(conn.assigns.pending_account, display_name) do
      {:ok, player} ->
        conn
        |> put_flash(
          :info,
          "Carrier detected. Welcome aboard, #{player.display_name}. Your dial-in code is waiting under Settings."
        )
        |> PlayerAuth.log_in_player(player)

      {:error, {:invalid, message}} ->
        render(conn, :new, form: player_form(params, display_name: message))

      {:error, _reason} ->
        conn
        |> put_flash(:error, "The universe is unreachable. Try again in a moment.")
        |> render(:new, form: player_form(params))
    end
  end

  defp player_form(params, errors \\ []) do
    Phoenix.Component.to_form(params,
      as: :player,
      errors: Enum.map(errors, fn {field, message} -> {field, {message, []}} end)
    )
  end

  defp require_pending_account(conn, _opts) do
    case get_session(conn, :pending_account) do
      %Account{} = account ->
        assign(conn, :pending_account, account)

      _other ->
        conn
        |> put_flash(:error, "Sign in with GitHub to create your player.")
        |> redirect(to: ~p"/")
        |> halt()
    end
  end
end
