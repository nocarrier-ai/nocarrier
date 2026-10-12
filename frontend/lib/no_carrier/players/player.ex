defmodule NoCarrier.Players.Player do
  @enforce_keys [:id, :display_name, :dial_in_code, :oauth_id, :login]
  defstruct [:id, :display_name, :dial_in_code, :oauth_id, :login, :name, :email, :avatar_url]
end
