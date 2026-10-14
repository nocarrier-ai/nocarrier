defmodule NoCarrier.Telnet.StatusTest do
  use ExUnit.Case, async: true

  alias NoCarrier.Telnet.Status
  alias NoCarrier.Universe

  @universe %Universe.Status{
    seed: 7,
    sectors: 1000,
    tick: 41_882,
    tick_period_ms: 60_000,
    ports: 316,
    planets: 1204,
    players: 12
  }

  @relay %{dialed_in: 3, node: :relay@test, online: 2}

  test "formats the relay status from a universe status" do
    assert Status.lines({:ok, @universe}, @relay) == [
             "RELAY STATUS",
             "  universe   seed 7   1,000 sectors   316 ports   1,204 planets",
             "  tick       41,882   one tick every 60s",
             "  players    12 registered   3 dialed in",
             "  relay      relay@test   2 online"
           ]
  end

  test "reports an unreachable universe without universe numbers" do
    assert Status.lines({:error, :unavailable}, @relay) == [
             "RELAY STATUS",
             "  universe   unreachable",
             "  players    3 dialed in",
             "  relay      relay@test   2 online"
           ]
  end

  test "greets with the seed and a comma-grouped tick" do
    greeting = Enum.join(Status.greeting({:ok, @universe}), "\n")
    assert greeting =~ "seed 7"
    assert greeting =~ "41,882"
  end

  test "greets an unreachable universe with placeholders" do
    greeting = Enum.join(Status.greeting({:error, :unavailable}), "\n")
    assert greeting =~ "unreachable"
    assert greeting =~ "---"
  end
end
