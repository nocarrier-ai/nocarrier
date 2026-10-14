defmodule NoCarrier.Doctrine.Service.Memory do
  @behaviour NoCarrier.Doctrine.Service

  use Agent

  def start_link(_opts \\ []) do
    Agent.start_link(fn -> %{doctrines: %{}, failure: nil} end, name: __MODULE__)
  end

  @impl true
  def doctrine(avatar_id) do
    Agent.get(__MODULE__, fn
      %{failure: failure} when not is_nil(failure) ->
        {:error, failure}

      state ->
        case Map.fetch(state.doctrines, avatar_id) do
          {:ok, entry} -> {:ok, entry}
          :error -> :not_found
        end
    end)
  end

  @impl true
  def update(avatar_id, orders, hooks) do
    Agent.get_and_update(__MODULE__, fn state ->
      if state.failure do
        {{:error, state.failure}, state}
      else
        revision =
          case Map.fetch(state.doctrines, avatar_id) do
            {:ok, entry} -> entry.revision + 1
            :error -> 1
          end

        entry = %{revision: revision, tick: 0, orders: orders, hooks: hooks}

        {{:ok, %{revision: revision, tick: 0, hash: "hash-#{revision}"}},
         %{state | doctrines: Map.put(state.doctrines, avatar_id, entry)}}
      end
    end)
  end

  def put(avatar_id, entry) do
    Agent.update(__MODULE__, &%{&1 | doctrines: Map.put(&1.doctrines, avatar_id, entry)})
  end

  def fail_with(reason) do
    Agent.update(__MODULE__, &%{&1 | failure: reason})
  end

  def doctrines do
    Agent.get(__MODULE__, & &1.doctrines)
  end
end
