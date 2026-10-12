defmodule NoCarrier.Players.OAuthID do
  @enforce_keys [:provider, :uid]
  defstruct [:provider, :uid]

  @type t :: %__MODULE__{provider: String.t(), uid: String.t()}

  def key(%__MODULE__{provider: provider, uid: uid}), do: "oauth.#{provider}.#{uid}"
end
