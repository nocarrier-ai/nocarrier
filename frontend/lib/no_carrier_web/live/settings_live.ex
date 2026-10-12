defmodule NoCarrierWeb.SettingsLive do
  use NoCarrierWeb, :live_view

  @impl true
  def mount(_params, _session, socket) do
    {:ok, assign(socket, page_title: "Settings")}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app flash={@flash} current_scope={@current_scope}>
      <div id="settings" class="space-y-8">
        <.header>
          <span class="text-primary glow uppercase tracking-[0.2em]">Settings</span>
          <:subtitle>Relay access for Admiral {@current_scope.player.display_name}.</:subtitle>
        </.header>

        <.panel id="dial-in-code" title="Dial-in code">
          <p class="text-sm leading-relaxed text-base-content/70">
            This is your dial-in code. When you connect to the relay via telnet, enter it at
            the <span class="text-base-content">ENTER DIAL CODE</span> prompt to reach your flagship.
            Keep it private: anyone holding it can issue orders as you.
          </p>

          <div class="mt-5 flex flex-wrap items-center gap-4">
            <code
              id="dial-in-code-value"
              class="select-all text-2xl text-primary glow tracking-[0.25em] sm:text-3xl"
            >
              {@current_scope.player.dial_in_code}
            </code>
            <button
              id="copy-dial-in-code"
              type="button"
              phx-hook="CopyToClipboard"
              data-copy={@current_scope.player.dial_in_code}
              class="group btn btn-primary btn-sm uppercase tracking-[0.2em]"
            >
              <.icon name="hero-clipboard-document" class="size-4 group-data-copied:hidden" />
              <.icon name="hero-check" class="hidden size-4 group-data-copied:inline-block" />
              <span class="group-data-copied:hidden">Copy</span>
              <span class="hidden group-data-copied:inline">Copied</span>
            </button>
          </div>

          <.prompt class="mt-5">telnet nocarrier.ai</.prompt>
        </.panel>
      </div>
    </Layouts.app>
    """
  end
end
