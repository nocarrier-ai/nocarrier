defmodule NoCarrier.Players.Account do
  @enforce_keys [:oauth_id, :login]
  defstruct [:oauth_id, :login, :name, :email, :avatar_url]
end
