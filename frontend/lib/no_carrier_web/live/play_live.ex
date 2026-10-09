defmodule NoCarrierWeb.PlayLive do
  use NoCarrierWeb, :live_view

  @impl true
  def mount(_params, _session, socket) do
    {:ok, assign(socket, page_title: "Play", node: Node.self(), peer_count: length(Node.list()))}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app flash={@flash} current_scope={@current_scope}>
      <div id="play" class="space-y-8">
        <.header>
          <span class="text-primary glow uppercase tracking-[0.2em]">
            Admiral {@current_scope.player.login}
          </span>
          <:subtitle>Flagship standing by. No standing orders on file.</:subtitle>
          <:actions>
            <button
              id="issue-orders"
              class="btn btn-primary btn-sm uppercase tracking-[0.2em]"
              disabled
            >
              Issue orders
            </button>
          </:actions>
        </.header>

        <div class="grid gap-6 md:grid-cols-2">
          <.panel id="flagship" title="Flagship">
            <dl class="grid grid-cols-[auto_1fr] gap-x-8 gap-y-2 text-sm">
              <dt class="uppercase tracking-widest text-base-content/50">Sector</dt>
              <dd class="text-primary">---</dd>
              <dt class="uppercase tracking-widest text-base-content/50">Tick</dt>
              <dd class="text-primary">---</dd>
              <dt class="uppercase tracking-widest text-base-content/50">Hull</dt>
              <dd class="text-primary">---</dd>
              <dt class="uppercase tracking-widest text-base-content/50">Cargo</dt>
              <dd class="text-primary">---</dd>
              <dt class="uppercase tracking-widest text-base-content/50">Credits</dt>
              <dd class="text-primary">---</dd>
            </dl>
          </.panel>

          <.panel id="doctrine" title="Doctrine">
            <p class="text-sm text-base-content/70 leading-relaxed">
              Your admiral has no standing orders yet. The universe is advancing without them.
            </p>
          </.panel>
        </div>

        <.panel id="log" title="Log">
          <ol class="space-y-1 text-sm">
            <li>
              <span class="text-base-content/40">[----]</span> Awaiting link to the universe.
            </li>
          </ol>
          <.prompt class="mt-4">standing by</.prompt>
        </.panel>

        <p id="node-status" class="text-xs text-base-content/40">
          node {@node} · {@peer_count} connected peer(s)
        </p>
      </div>
    </Layouts.app>
    """
  end
end
