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
    player = Players.player_from_auth(auth)

    conn
    |> put_flash(:info, "Carrier detected. Welcome aboard, #{player.login}.")
    |> PlayerAuth.log_in_player(player)
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
