defmodule NoCarrier.Telnet.Banner do
  @reset "\e[0m"
  @amber "\e[1;33m"
  @dark_amber "\e[0;33m"
  @cyan "\e[0;36m"
  @bright_cyan "\e[1;36m"
  @white "\e[0;37m"

  @logo [
    @amber <> "  █▄   █ ▄▀▀▀▀▄   ▄▀▀▀▀▀ ▄▀▀▀▀▄ █▀▀▀▀▄ █▀▀▀▀▄ ▀██▀ █▀▀▀▀▀ █▀▀▀▀▄",
    @amber <> "  █▀█▄ █ █    █   █      █    █ █▄▄▄▄▀ █▄▄▄▄▀  ██  █▄▄▄▄  █▄▄▄▄▀",
    @dark_amber <> "  █  ▀██ █    █   █      █▀▀▀▀█ █  ▀▄  █  ▀▄   ██  █      █  ▀▄ ",
    @dark_amber <> "  █    █ ▀▄▄▄▄▀   ▀▄▄▄▄▄ █    █ █    █ █    █ ▄██▄ █▄▄▄▄▄ █    █" <> @reset,
    ""
  ]

  @relay [
    @cyan <> "  ┌───────────────────────────────────────────────────────┐",
    @cyan <>
      "  │" <>
      @bright_cyan <> "     Galactic Communications Relay Access Point        " <> @cyan <> "│",
    @cyan <>
      "  │" <> @white <> "  A Hayes-compatible subspace modem. Type ATZ if lost. " <> @cyan <> "│",
    @cyan <> "  └───────────────────────────────────────────────────────┘" <> @reset,
    ""
  ]

  def lines(universe, tick) do
    @logo ++
      @relay ++
      [
        "  " <>
          @cyan <>
          "universe " <>
          @amber <> universe <> @reset <> "   " <> @cyan <> "tick " <> @amber <> tick <> @reset,
        ""
      ]
  end
end
