defmodule NoCarrierWeb.Layouts do
  use NoCarrierWeb, :html

  embed_templates "layouts/*"

  attr :flash, :map, required: true
  attr :current_scope, :map, default: nil
  slot :inner_block, required: true

  def app(assigns) do
    ~H"""
    <header class="navbar border-b border-base-300 px-4 sm:px-6 lg:px-8">
      <div class="flex-1">
        <.link navigate={~p"/"} class="flex w-fit items-center gap-3">
          <img src={~p"/images/nocarrier-mark-dark.svg"} width="32" height="32" alt="" />
          <span class="text-sm font-semibold uppercase tracking-[0.3em] text-primary glow">
            No Carrier
          </span>
        </.link>
      </div>
      <nav class="flex-none">
        <ul class="flex items-center gap-2">
          <%= if @current_scope do %>
            <li>
              <.link navigate={~p"/play"} class="btn btn-ghost btn-sm uppercase tracking-[0.2em]">
                Play
              </.link>
            </li>
            <li class="hidden items-center gap-2 px-2 text-sm text-base-content/70 sm:flex">
              <img
                :if={@current_scope.player.avatar_url}
                src={@current_scope.player.avatar_url}
                width="24"
                height="24"
                alt=""
                class="size-6"
              />
              <span>{@current_scope.player.login}</span>
            </li>
            <li>
              <.link
                href={~p"/log-out"}
                method="delete"
                class="btn btn-ghost btn-sm uppercase tracking-[0.2em]"
              >
                Log out
              </.link>
            </li>
          <% else %>
            <li>
              <a href={~p"/auth/github"} class="btn btn-primary btn-sm uppercase tracking-[0.2em]">
                Sign in
              </a>
            </li>
          <% end %>
        </ul>
      </nav>
    </header>

    <main class="px-4 py-12 sm:px-6 lg:px-8">
      <div class="mx-auto max-w-4xl space-y-10">
        {render_slot(@inner_block)}
      </div>
    </main>

    <.flash_group flash={@flash} />
    """
  end

  attr :flash, :map, required: true
  attr :id, :string, default: "flash-group"

  def flash_group(assigns) do
    ~H"""
    <div id={@id} aria-live="polite">
      <.flash kind={:info} flash={@flash} />
      <.flash kind={:error} flash={@flash} />

      <.flash
        id="client-error"
        kind={:error}
        title={gettext("We can't find the internet")}
        phx-disconnected={
          show(".phx-client-error #client-error")
          |> JS.remove_attribute("hidden", to: ".phx-client-error #client-error")
        }
        phx-connected={hide("#client-error") |> JS.set_attribute({"hidden", ""})}
        hidden
      >
        {gettext("Attempting to reconnect")}
        <.icon name="hero-arrow-path" class="ml-1 size-3 motion-safe:animate-spin" />
      </.flash>

      <.flash
        id="server-error"
        kind={:error}
        title={gettext("Something went wrong!")}
        phx-disconnected={
          show(".phx-server-error #server-error")
          |> JS.remove_attribute("hidden", to: ".phx-server-error #server-error")
        }
        phx-connected={hide("#server-error") |> JS.set_attribute({"hidden", ""})}
        hidden
      >
        {gettext("Attempting to reconnect")}
        <.icon name="hero-arrow-path" class="ml-1 size-3 motion-safe:animate-spin" />
      </.flash>
    </div>
    """
  end
end
