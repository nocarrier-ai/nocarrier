defmodule NoCarrier.Players.Service.Memory do
  @behaviour NoCarrier.Players.Service

  use Agent

  alias NoCarrier.Players.OAuthID

  def start_link(_opts \\ []) do
    Agent.start_link(fn -> %{players: %{}, by_oauth: %{}, failure: nil} end, name: __MODULE__)
  end

  @impl true
  def lookup_player(%OAuthID{} = oauth_id) do
    Agent.get(__MODULE__, fn
      %{failure: failure} when not is_nil(failure) ->
        {:error, failure}

      state ->
        case Map.fetch(state.by_oauth, OAuthID.key(oauth_id)) do
          {:ok, player_id} -> {:ok, Map.fetch!(state.players, player_id)}
          :error -> :not_found
        end
    end)
  end

  @impl true
  def create_player(command) do
    Agent.get_and_update(__MODULE__, fn state ->
      key = OAuthID.key(command.oauth_id)

      cond do
        state.failure ->
          {{:error, state.failure}, state}

        Map.has_key?(state.by_oauth, key) or Map.has_key?(state.players, command.player_id) ->
          {{:error, :already_registered}, state}

        true ->
          entry = %{
            player_id: command.player_id,
            display_name: command.display_name,
            dial_in_code: command.dial_in_code
          }

          {{:ok, entry},
           %{
             state
             | players: Map.put(state.players, command.player_id, entry),
               by_oauth: Map.put(state.by_oauth, key, command.player_id)
           }}
      end
    end)
  end

  def fail_with(reason) do
    Agent.update(__MODULE__, &%{&1 | failure: reason})
  end

  def players do
    Agent.get(__MODULE__, &Map.values(&1.players))
  end
end
