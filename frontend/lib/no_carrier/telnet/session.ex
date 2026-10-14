defmodule NoCarrier.Telnet.Session do
  use ThousandIsland.Handler

  require Logger

  alias NoCarrier.Doctrine
  alias NoCarrier.News
  alias NoCarrier.Players
  alias NoCarrier.Telnet.Sessions
  alias NoCarrier.Telnet.Status
  alias NoCarrier.Universe
  alias ThousandIsland.Socket

  @claim_attempts 50
  @claim_retry_ms 100
  @will_echo_and_suppress_go_ahead <<255, 251, 1, 255, 251, 3>>
  @clip_length 66

  @help [
    "  ?                 this help",
    "  /status           relay and universe status",
    "  doctrine          the standing orders on file, or your draft",
    "  doctrine hooks    the event hooks (attacked, moved)",
    "  doctrine edit N   rewrite draft section N; one past the end adds one",
    "  doctrine edit H   rewrite draft hook H; an empty body clears it",
    "  doctrine send     transmit the draft to the flagship",
    "  doctrine discard  abandon the draft",
    "  /quit             hang up",
    ""
  ]

  @impl ThousandIsland.Handler
  def handle_connection(socket, _options) do
    _ = Socket.send(socket, @will_echo_and_suppress_go_ahead)
    say(socket, Status.greeting(Universe.status()))
    prompt(socket, %{stage: :dial, line: "", after_cr: false, claims: 0, player: nil, draft: nil})
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

  def handle_info({:news, %{tick: tick, text: text}}, {socket, state}) do
    say(socket, ["", "  #{Status.commas(tick)}  #{text}", ""])
    result = prompt(socket, state)
    _ = Socket.send(socket, state.line)
    reply(result, socket)
  end

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

  defp handle_line(line, socket, %{stage: {:confirm, _words}} = state),
    do: confirm(line, socket, state)

  defp handle_line("", socket, state), do: prompt(socket, state)

  defp handle_line(line, socket, %{stage: {:editing, edit}} = state),
    do: edit_line(line, socket, edit, state)

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
    :ok = News.subscribe(player.player_id)

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

  defp command("doctrine", socket, state), do: show_doctrine(socket, state)

  defp command("doctrine hooks", socket, state), do: show_hooks(socket, state)

  defp command("doctrine send", socket, state), do: send_draft(socket, state)

  defp command("doctrine discard", socket, state), do: discard_draft(socket, state)

  defp command("doctrine edit " <> target, socket, state),
    do: edit_doctrine(String.trim(target), socket, state)

  defp command(_line, socket, state) do
    say(socket, ["UNKNOWN COMMAND (? for help)", ""])
    prompt(socket, state)
  end

  defp show_doctrine(socket, %{draft: %{} = draft} = state) do
    header = ~s{DOCTRINE DRAFT (unsent; "doctrine send" transmits)}
    say(socket, [header | section_lines(draft.orders)] ++ [""])
    prompt(socket, state)
  end

  defp show_doctrine(socket, %{player: player} = state) do
    case Doctrine.doctrine(avatar_id(player)) do
      {:ok, doc} ->
        header = "DOCTRINE rev #{doc.revision} (updated tick #{Status.commas(doc.tick)})"
        say(socket, [header | section_lines(doc.orders)] ++ [""])
        prompt(socket, state)

      :not_found ->
        say(socket, [~s(NO DOCTRINE ON FILE. "doctrine edit 1" writes the first section.), ""])
        prompt(socket, state)

      {:error, _reason} ->
        say(socket, ["RELAY LINK DOWN. TRY AGAIN SHORTLY.", ""])
        prompt(socket, state)
    end
  end

  defp section_lines(orders) do
    orders
    |> Enum.with_index(1)
    |> Enum.map(fn {order, index} -> "  [#{index}] #{clip(order)}" end)
  end

  defp show_hooks(socket, %{draft: %{} = draft} = state) do
    say(socket, ["DOCTRINE HOOKS DRAFT (unsent)" | hook_lines(draft.hooks)] ++ [""])
    prompt(socket, state)
  end

  defp show_hooks(socket, %{player: player} = state) do
    case Doctrine.doctrine(avatar_id(player)) do
      {:ok, doc} ->
        say(socket, ["DOCTRINE HOOKS rev #{doc.revision}" | hook_lines(doc.hooks)] ++ [""])
        prompt(socket, state)

      :not_found ->
        say(socket, [
          "NO DOCTRINE ON FILE. HOOKS AVAILABLE: #{Enum.join(Doctrine.hooks(), ", ")}.",
          ""
        ])

        prompt(socket, state)

      {:error, _reason} ->
        say(socket, ["RELAY LINK DOWN. TRY AGAIN SHORTLY.", ""])
        prompt(socket, state)
    end
  end

  defp hook_lines(hooks) do
    width = Doctrine.hooks() |> Enum.map(&String.length/1) |> Enum.max()

    for hook <- Doctrine.hooks() do
      "  #{String.pad_trailing("[#{hook}]", width + 2)} #{hook_body(hooks[hook])}"
    end
  end

  defp hook_body(nil), do: "(not set)"
  defp hook_body(order), do: clip(order)

  defp clip(order) do
    if String.length(order) > @clip_length,
      do: String.slice(order, 0, @clip_length) <> "...",
      else: order
  end

  defp edit_doctrine(target, socket, %{draft: %{} = draft} = state),
    do: start_editor(target, socket, state, draft.orders, draft.hooks)

  defp edit_doctrine(target, socket, %{player: player} = state) do
    case Doctrine.doctrine(avatar_id(player)) do
      {:ok, doc} ->
        start_editor(target, socket, state, doc.orders, doc.hooks)

      :not_found ->
        start_editor(target, socket, state, [], %{})

      {:error, _reason} ->
        say(socket, ["RELAY LINK DOWN. TRY AGAIN SHORTLY.", ""])
        prompt(socket, state)
    end
  end

  defp start_editor(target, socket, state, orders, hooks) do
    target = String.downcase(target)
    last = length(orders) + 1

    cond do
      target in Doctrine.hooks() and orders == [] ->
        say(socket, [
          ~s(HOOKS NEED STANDING ORDERS. "doctrine edit 1" writes the first section.),
          ""
        ])

        prompt(socket, state)

      target in Doctrine.hooks() ->
        open_editor(socket, state, {:hook, target}, "EDITING HOOK #{target}.", orders, hooks)

      true ->
        case Integer.parse(target) do
          {n, ""} when n >= 1 and n <= last ->
            heading = if n == last, do: "EDITING NEW SECTION #{n}.", else: "EDITING SECTION #{n}."
            open_editor(socket, state, {:section, n}, heading, orders, hooks)

          _other ->
            say(socket, [
              "SECTION MUST BE 1..#{last} OR A HOOK (#{Enum.join(Doctrine.hooks(), ", ")}).",
              ""
            ])

            prompt(socket, state)
        end
    end
  end

  defp open_editor(socket, state, target, heading, orders, hooks) do
    say(socket, [heading <> ~s( End with a single "." on its own line.)])
    edit = %{target: target, lines: [], orders: orders, hooks: hooks}
    prompt(socket, %{state | stage: {:editing, edit}})
  end

  defp edit_line(".", socket, edit, state) do
    text = edit.lines |> Enum.reverse() |> Enum.join(" ") |> String.trim()
    submit_edit(edit.target, text, socket, edit, state)
  end

  defp edit_line(line, socket, edit, state),
    do: prompt(socket, %{state | stage: {:editing, %{edit | lines: [line | edit.lines]}}})

  defp submit_edit({:section, _n}, "", socket, _edit, state), do: nothing_saved(socket, state)

  defp submit_edit({:section, n}, text, socket, edit, state) do
    orders =
      if n > length(edit.orders),
        do: edit.orders ++ [text],
        else: List.replace_at(edit.orders, n - 1, text)

    save_draft(socket, state, orders, edit.hooks, "section #{n} is #{counted(text)}")
  end

  defp submit_edit({:hook, hook}, "", socket, edit, state) do
    if Map.has_key?(edit.hooks, hook) do
      save_draft(socket, state, edit.orders, Map.delete(edit.hooks, hook), "hook #{hook} cleared")
    else
      nothing_saved(socket, state)
    end
  end

  defp submit_edit({:hook, hook}, text, socket, edit, state) do
    hooks = Map.put(edit.hooks, hook, text)
    save_draft(socket, state, edit.orders, hooks, "hook #{hook} is #{counted(text)}")
  end

  defp counted(text), do: "#{Doctrine.word_count(text)} words (limit #{Doctrine.word_limit()})"

  defp save_draft(socket, state, orders, hooks, saved) do
    say(socket, [~s(DRAFT SAVED: #{saved}. "doctrine send" transmits.), ""])
    prompt(socket, %{state | stage: :command, draft: %{orders: orders, hooks: hooks}})
  end

  defp nothing_saved(socket, state) do
    say(socket, ["NOTHING SAVED.", ""])
    prompt(socket, %{state | stage: :command})
  end

  defp send_draft(socket, %{draft: %{} = draft} = state) do
    prompt(socket, %{state | stage: {:confirm, Doctrine.word_count(draft.orders, draft.hooks)}})
  end

  defp send_draft(socket, state) do
    say(socket, [~s(NO DRAFT TO SEND. "doctrine edit ..." starts one.), ""])
    prompt(socket, state)
  end

  defp discard_draft(socket, %{draft: %{}} = state) do
    say(socket, ["DRAFT DISCARDED.", ""])
    prompt(socket, %{state | draft: nil})
  end

  defp discard_draft(socket, state) do
    say(socket, ["NO DRAFT.", ""])
    prompt(socket, state)
  end

  defp confirm(line, socket, state) do
    case String.downcase(line) do
      yes when yes in ["", "y", "yes"] ->
        transmit(socket, state)

      no when no in ["n", "no"] ->
        say(socket, ["NOTHING SENT. DRAFT KEPT.", ""])
        prompt(socket, %{state | stage: :command})

      _other ->
        prompt(socket, state)
    end
  end

  defp transmit(socket, %{player: player, draft: draft} = state) do
    case Doctrine.update(avatar_id(player), draft.orders, draft.hooks) do
      {:ok, %{revision: revision}} ->
        say(socket, ["", "TRANSMITTING............ ACCEPTED AS rev #{revision}", ""])
        prompt(socket, %{state | stage: :command, draft: nil})

      {:error, {:invalid, reason}} ->
        say(socket, ["", "REJECTED: #{reason}", "DRAFT KEPT.", ""])
        prompt(socket, %{state | stage: :command})

      {:error, :conflict} ->
        say(socket, [
          "",
          ~s(REJECTED: DOCTRINE CHANGED UNDER YOU. CHECK "doctrine" AND RESUBMIT.),
          "DRAFT KEPT.",
          ""
        ])

        prompt(socket, %{state | stage: :command})

      {:error, _reason} ->
        say(socket, ["", "RELAY LINK DOWN. NOTHING SENT. DRAFT KEPT.", ""])
        prompt(socket, %{state | stage: :command})
    end
  end

  defp avatar_id(player), do: player.player_id

  defp hang_up(socket, state) do
    say(socket, ["NO CARRIER"])
    {:close, state}
  end

  defp prompt(socket, %{stage: :dial} = state) do
    _ = Socket.send(socket, "ENTER DIAL CODE: ")
    {:continue, state}
  end

  defp prompt(socket, %{stage: {:editing, _edit}} = state) do
    _ = Socket.send(socket, "> ")
    {:continue, state}
  end

  defp prompt(socket, %{stage: {:confirm, words}} = state) do
    _ = Socket.send(socket, "#{words} words total. Submit? (Y/n) ")
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
