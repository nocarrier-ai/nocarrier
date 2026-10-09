defmodule NoCarrier.Players.Player do
  @enforce_keys [:id, :provider, :uid, :login]
  defstruct [:id, :provider, :uid, :login, :name, :email, :avatar_url]
end
