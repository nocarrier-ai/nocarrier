defmodule NoCarrier.TelnetTest do
  use ExUnit.Case, async: false

  import NoCarrier.PlayersFixtures
  import NoCarrier.TelnetHelpers

  alias NoCarrier.Players.Service.Memory

  @code "7719-0402-5518"

  setup do
    start_supervised!(Memory)
    register_player()
    :ok
  end

  test "greets with the ANSI banner and asks for a dial code" do
    socket = connect()
    greeting = read_until(socket, "ENTER DIAL CODE: ")
    plain = String.replace(greeting, ~r/\e\[[\d;]*m/, "")

    assert greeting =~ "\e[1;33m  █▄   █ ▄▀▀▀▀▄   ▄▀▀▀▀▀ ▄▀▀▀▀▄ █▀▀▀▀▄ █▀▀▀▀▄ ▀██▀ █▀▀▀▀▀ █▀▀▀▀▄"
    assert plain =~ "Galactic Communications Relay Access Point"
    assert plain =~ "universe unreachable   tick ---"
    assert String.ends_with?(greeting, "\e[0m\r\n\r\nENTER DIAL CODE: ")
  end

  test "connects the player who enters their dial-in code" do
    socket = connect()
    connected = dial(socket, @code)

    assert connected =~ "CONNECT 96000"
    assert connected =~ "Welcome back, Ada."
    assert connected =~ "RELAY STATUS"
    assert connected =~ "1 dialed in"
    assert connected =~ "relay      #{node()}   1 online"
  end

  test "echoes what the player types and erases on backspace" do
    socket = connect()
    read_until(socket, "ENTER DIAL CODE: ")
    :ok = :gen_tcp.send(socket, "7719-0402-5519" <> <<127>> <> "8\r\n")

    connected = read_until(socket, "COMMAND (? for help)> ")
    assert connected =~ "7719-0402-5519\b \b8\r\n"
    assert connected =~ "CONNECT 96000"
  end

  test "strips telnet negotiation from the input" do
    socket = connect()
    read_until(socket, "ENTER DIAL CODE: ")

    :ok =
      :gen_tcp.send(
        socket,
        <<255, 253, 1, 255, 250, 31, 0, 80, 0, 24, 255, 240>> <> @code <> "\r\n"
      )

    assert read_until(socket, "COMMAND (? for help)> ") =~ "CONNECT 96000"
  end

  test "answers ? with help, /status with the relay status, and rejects the rest" do
    socket = connect()
    dial(socket, @code)

    send_line(socket, "?")
    assert read_until(socket, "COMMAND (? for help)> ") =~ "/quit             hang up"

    send_line(socket, "/status")
    assert read_until(socket, "COMMAND (? for help)> ") =~ "RELAY STATUS"

    send_line(socket, "warp 42")
    assert read_until(socket, "COMMAND (? for help)> ") =~ "UNKNOWN COMMAND"
  end

  test "hangs up on /quit" do
    socket = connect()
    dial(socket, @code)
    send_line(socket, "/quit")

    assert drain(socket) =~ "NO CARRIER\r\n"
  end

  test "hangs up on a wrong dial code" do
    socket = connect()
    read_until(socket, "ENTER DIAL CODE: ")
    send_line(socket, "0000-0000-0000")

    assert drain(socket) =~ "NO CARRIER\r\n"
  end

  @tag :capture_log
  test "hangs up with NO DIALTONE when the universe is unreachable" do
    Memory.fail_with(:unavailable)
    socket = connect()
    read_until(socket, "ENTER DIAL CODE: ")
    send_line(socket, @code)

    assert drain(socket) =~ "NO DIALTONE\r\n"
  end

  test "boots the existing session when the player dials in again" do
    first = connect()
    dial(first, @code)
    second = connect()

    assert dial(second, @code) =~ "CONNECT 96000"
    assert drain(first) =~ "ANOTHER TERMINAL DIALED IN WITH YOUR CODE.\r\nNO CARRIER\r\n"
  end

  test "frees the session when the player hangs up" do
    first = connect()
    dial(first, @code)
    send_line(first, "/quit")
    drain(first)

    second = connect()
    assert dial(second, @code) =~ "1 dialed in"
  end
end
