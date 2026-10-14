defmodule NoCarrier.Telnet.Sessions do
  @registry NoCarrier.HordeRegistry

  def claim(player_id), do: Horde.Registry.register(@registry, key(player_id), node())

  def dialed_in do
    @registry
    |> Horde.Registry.select([{{key(:_), :_, :_}, [], [true]}])
    |> length()
  end

  defp key(player_id), do: {:telnet, player_id}
end
