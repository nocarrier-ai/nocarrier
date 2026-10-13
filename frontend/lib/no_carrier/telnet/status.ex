defmodule NoCarrier.Telnet.Status do
  alias NoCarrier.Telnet.Banner
  alias NoCarrier.Universe

  def greeting({:ok, %Universe.Status{} = universe}),
    do: Banner.lines("seed #{universe.seed}", commas(universe.tick))

  def greeting({:error, _reason}), do: Banner.lines("unreachable", "---")

  def lines({:ok, %Universe.Status{} = universe}, relay) do
    [
      "RELAY STATUS",
      "  universe   seed #{universe.seed}   #{commas(universe.sectors)} sectors   " <>
        "#{commas(universe.ports)} ports   #{commas(universe.planets)} planets",
      "  tick       #{commas(universe.tick)}   one tick every " <>
        "#{div(universe.tick_period_ms, 1000)}s",
      "  players    #{commas(universe.players)} registered   #{relay.dialed_in} dialed in",
      relay_line(relay)
    ]
  end

  def lines({:error, _reason}, relay) do
    [
      "RELAY STATUS",
      "  universe   unreachable",
      "  players    #{relay.dialed_in} dialed in",
      relay_line(relay)
    ]
  end

  defp relay_line(relay), do: "  relay      #{relay.node}   #{relay.online} online"

  defp commas(n) do
    n
    |> Integer.to_string()
    |> String.reverse()
    |> String.replace(~r/(\d{3})(?=\d)/, "\\1,")
    |> String.reverse()
  end
end
