defmodule NoCarrier.Players do
  alias NoCarrier.Players.Player

  def player_from_auth(%Ueberauth.Auth{} = auth) do
    %Player{
      id: "#{auth.provider}:#{auth.uid}",
      provider: auth.provider,
      uid: to_string(auth.uid),
      login: auth.info.nickname,
      name: auth.info.name,
      email: auth.info.email,
      avatar_url: auth.info.image
    }
  end
end
