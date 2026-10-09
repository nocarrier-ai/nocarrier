defmodule NoCarrier.Players.Scope do
  alias NoCarrier.Players.Player

  defstruct player: nil

  def for_player(%Player{} = player), do: %__MODULE__{player: player}
  def for_player(nil), do: nil
end
