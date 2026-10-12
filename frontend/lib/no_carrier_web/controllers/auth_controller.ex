defmodule NoCarrierWeb.AuthController do
  use NoCarrierWeb, :controller

  alias NoCarrier.Players
  alias NoCarrierWeb.PlayerAuth

  plug Ueberauth

  def request(conn, _params) do
    conn
    |> put_flash(:error, "Unknown sign-in provider.")
    |> redirect(to: ~p"/")
  end

  def callback(%{assigns: %{ueberauth_failure: failure}} = conn, _params) do
    conn
    |> put_flash(:error, "Sign-in failed: #{failure_message(failure)}")
    |> redirect(to: ~p"/")
  end

  def callback(%{assigns: %{ueberauth_auth: auth}} = conn, _params) do
    account = Players.account_from_auth(auth)

    case Players.find_player(account) do
      {:ok, player} ->
        conn
        |> put_flash(:info, "Carrier detected. Welcome back, #{player.display_name}.")
        |> PlayerAuth.log_in_player(player)

      :not_found ->
        conn
        |> put_session(:pending_account, account)
        |> redirect(to: ~p"/players/new")

      {:error, _reason} ->
        conn
        |> put_flash(
          :error,
          "Carrier detected, but the universe is unreachable. Try again in a moment."
        )
        |> redirect(to: ~p"/")
    end
  end

  def delete(conn, _params) do
    conn
    |> put_flash(:info, "NO CARRIER. You have been logged out.")
    |> PlayerAuth.log_out_player()
  end

  defp failure_message(%Ueberauth.Failure{errors: errors}) do
    Enum.map_join(errors, ", ", & &1.message)
  end
end
