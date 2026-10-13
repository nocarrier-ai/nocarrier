defmodule NoCarrier.Telnet.Session do
  use ThousandIsland.Handler

  require Logger

  alias NoCarrier.Players
  alias NoCarrier.Telnet.Sessions
  alias NoCarrier.Telnet.Status
  alias NoCarrier.Universe
  alias ThousandIsland.Socket

  @claim_attempts 50
  @claim_retry_ms 100
  @will_echo_and_suppress_go_ahead <<255, 251, 1, 255, 251, 3>>

  @help [
    "  ?         this help",
    "  /status   relay and universe status",
    "  /quit     hang up",
    ""
  ]

  @impl ThousandIsland.Handler
  def handle_connection(socket, _options) do
    _ = Socket.send(socket, @will_echo_and_suppress_go_ahead)
    say(socket, Status.greeting(Universe.status()))
    prompt(socket, %{stage: :dial, line: "", after_cr: false, claims: 0, player: nil})
  end

  @impl ThousandIsland.Handler
  def handle_data(data, socket, state) do
    data
    |> strip_telnet()
    |> :binary.bin_to_list()
    |> Enum.reduce_while({:continue, state}, fn byte, {:continue, state} ->
      case handle_byte(byte, socket, state) do
        {:continue, _state} = continue -> {:cont, continue}
        {:close, _state} = close -> {:halt, close}
      end
    end)
  end

  @impl GenServer
  def handle_info(:claim, {socket, state}), do: socket |> claim(state) |> reply(socket)
  def handle_info({:booted, _by}, {socket, state}), do: booted(socket, state)

  def handle_info({:EXIT, _pid, {:name_conflict, _key, _registry, _winner}}, {socket, state}) do
    booted(socket, state)
  end

  defp booted(socket, state) do
    say(socket, ["", "", "ANOTHER TERMINAL DIALED IN WITH YOUR CODE.", "NO CARRIER"])
    reply({:close, state}, socket)
  end

  defp reply({:continue, state}, socket), do: {:noreply, {socket, state}}
  defp reply({:close, state}, socket), do: {:stop, {:shutdown, :local_closed}, {socket, state}}

  defp handle_byte(byte, socket, %{after_cr: true} = state) do
    state = %{state | after_cr: false}
    if byte in [?\n, 0], do: {:continue, state}, else: handle_byte(byte, socket, state)
  end

  defp handle_byte(?\r, socket, state), do: enter(socket, %{state | after_cr: true})
  defp handle_byte(?\n, socket, state), do: enter(socket, state)
  defp handle_byte(byte, socket, state) when byte in [8, 127], do: erase(socket, state)
  defp handle_byte(byte, _socket, state) when byte < 32, do: {:continue, state}

  defp handle_byte(byte, socket, state) do
    _ = Socket.send(socket, <<byte>>)
    {:continue, %{state | line: state.line <> <<byte>>}}
  end

  defp erase(_socket, %{line: ""} = state), do: {:continue, state}

  defp erase(socket, state) do
    _ = Socket.send(socket, "\b \b")
    {:continue, %{state | line: String.slice(state.line, 0..-2//1)}}
  end

  defp enter(socket, state) do
    _ = Socket.send(socket, "\r\n")
    handle_line(String.trim(state.line), socket, %{state | line: ""})
  end

  defp handle_line(_line, _socket, %{stage: :claiming} = state), do: {:continue, state}
  defp handle_line("", socket, state), do: prompt(socket, state)
  defp handle_line(code, socket, %{stage: :dial} = state), do: dial(code, socket, state)
  defp handle_line(line, socket, %{stage: :command} = state), do: command(line, socket, state)

  defp dial(code, socket, state) do
    case Players.dial_in(code) do
      {:ok, player} ->
        claim(socket, %{state | player: player})

      :not_found ->
        hang_up(socket, state)

      {:error, reason} ->
        Logger.warning("telnet dial-in failed", reason: inspect(reason))
        say(socket, ["NO DIALTONE"])
        {:close, state}
    end
  end

  defp claim(socket, %{player: player, claims: claims} = state) do
    case Sessions.claim(player.player_id) do
      {:ok, _owner} ->
        connected(socket, state)

      {:error, {:already_registered, pid}} when claims < @claim_attempts ->
        if claims == 0, do: send(pid, {:booted, self()})
        Process.send_after(self(), :claim, @claim_retry_ms)
        {:continue, %{state | stage: :claiming, claims: claims + 1}}

      {:error, {:already_registered, _pid}} ->
        say(socket, ["BUSY"])
        {:close, state}
    end
  end

  defp connected(socket, %{player: player} = state) do
    Logger.info("telnet dial-in", player_id: player.player_id)

    say(
      socket,
      ["CONNECT 96000", "", "Welcome back, #{player.display_name}.", ""] ++
        Status.lines(Universe.status(), relay()) ++ [""]
    )

    prompt(socket, %{state | stage: :command})
  end

  defp command("/quit", socket, state), do: hang_up(socket, state)

  defp command("?", socket, state) do
    say(socket, @help)
    prompt(socket, state)
  end

  defp command("/status", socket, state) do
    say(socket, Status.lines(Universe.status(), relay()) ++ [""])
    prompt(socket, state)
  end

  defp command(_line, socket, state) do
    say(socket, ["UNKNOWN COMMAND (? for help)", ""])
    prompt(socket, state)
  end

  defp hang_up(socket, state) do
    say(socket, ["NO CARRIER"])
    {:close, state}
  end

  defp prompt(socket, %{stage: :dial} = state) do
    _ = Socket.send(socket, "ENTER DIAL CODE: ")
    {:continue, state}
  end

  defp prompt(socket, state) do
    _ = Socket.send(socket, "COMMAND (? for help)> ")
    {:continue, state}
  end

  defp say(socket, lines), do: Socket.send(socket, Enum.map_join(lines, &(&1 <> "\r\n")))

  defp relay do
    %{dialed_in: Sessions.dialed_in(), node: node(), online: length(Node.list()) + 1}
  end

  defp strip_telnet(<<255, 250, rest::binary>>),
    do: rest |> skip_subnegotiation() |> strip_telnet()

  defp strip_telnet(<<255, cmd, _option, rest::binary>>) when cmd in 251..254,
    do: strip_telnet(rest)

  defp strip_telnet(<<255, _cmd, rest::binary>>), do: strip_telnet(rest)
  defp strip_telnet(<<byte, rest::binary>>), do: <<byte, strip_telnet(rest)::binary>>
  defp strip_telnet(<<>>), do: <<>>

  defp skip_subnegotiation(<<255, 240, rest::binary>>), do: rest
  defp skip_subnegotiation(<<_byte, rest::binary>>), do: skip_subnegotiation(rest)
  defp skip_subnegotiation(<<>>), do: <<>>
end
