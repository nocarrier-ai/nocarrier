defmodule NoCarrier.Universe do
  alias NoCarrier.Nats

  @bucket "universe-status"

  defmodule Status do
    @enforce_keys [:seed, :sectors, :tick, :tick_period_ms, :ports, :planets, :players]
    defstruct [:seed, :sectors, :tick, :tick_period_ms, :ports, :planets, :players]

    @type t :: %__MODULE__{
            seed: integer(),
            sectors: non_neg_integer(),
            tick: integer(),
            tick_period_ms: non_neg_integer(),
            ports: non_neg_integer(),
            planets: non_neg_integer(),
            players: non_neg_integer()
          }
  end

  def status do
    with {:ok, clock} <- read("clock") do
      decode(clock, counts())
    end
  end

  defp counts do
    case read("counts") do
      {:ok, counts} -> counts
      _error -> %{}
    end
  end

  defp read(key) do
    with {:ok, value} <- Nats.kv_get(@bucket, key),
         {:ok, map} when is_map(map) <- Jason.decode(value) do
      {:ok, map}
    else
      :not_found -> {:error, :unavailable}
      {:ok, _other} -> {:error, :malformed_reply}
      {:error, %Jason.DecodeError{}} -> {:error, :malformed_reply}
      {:error, _reason} = error -> error
    end
  end

  defp decode(
         %{
           "seed" => seed,
           "sectors" => sectors,
           "tick" => tick,
           "tick_period_ms" => tick_period_ms
         },
         counts
       )
       when is_integer(seed) and is_integer(sectors) and is_integer(tick) and
              is_integer(tick_period_ms) do
    {:ok,
     %Status{
       seed: seed,
       sectors: sectors,
       tick: tick,
       tick_period_ms: tick_period_ms,
       ports: count(counts, "ports"),
       planets: count(counts, "planets"),
       players: count(counts, "players")
     }}
  end

  defp decode(_clock, _counts), do: {:error, :malformed_reply}

  defp count(counts, key) do
    case Map.get(counts, key) do
      n when is_integer(n) and n >= 0 -> n
      _other -> 0
    end
  end
end
