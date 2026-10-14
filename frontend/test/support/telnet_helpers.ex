defmodule NoCarrier.TelnetHelpers do
  def connect do
    {:ok, {_ip, port}} = ThousandIsland.listener_info(NoCarrier.Telnet)
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])
    socket
  end

  def dial(socket, code) do
    read_until(socket, "ENTER DIAL CODE: ")
    send_line(socket, code)
    read_until(socket, "COMMAND (? for help)> ")
  end

  def send_line(socket, line), do: :ok = :gen_tcp.send(socket, line <> "\r\n")

  def read_until(socket, marker, acc \\ "") do
    if String.contains?(acc, marker) do
      acc
    else
      {:ok, data} = :gen_tcp.recv(socket, 0, 2_000)
      read_until(socket, marker, acc <> data)
    end
  end

  def drain(socket, acc \\ "") do
    case :gen_tcp.recv(socket, 0, 2_000) do
      {:ok, data} -> drain(socket, acc <> data)
      {:error, :closed} -> acc
    end
  end
end
