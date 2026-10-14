defmodule NoCarrier.Telnet.DoctrineTest do
  use ExUnit.Case, async: false

  import NoCarrier.PlayersFixtures
  import NoCarrier.TelnetHelpers

  alias NoCarrier.Doctrine.Service.Memory, as: DoctrineMemory
  alias NoCarrier.News
  alias NoCarrier.Players.Service.Memory, as: PlayersMemory

  @code "7719-0402-5518"
  @avatar_id "p-ada"
  @prompt "COMMAND (? for help)> "

  setup do
    start_supervised!(PlayersMemory)
    start_supervised!(DoctrineMemory)
    register_player()
    :ok
  end

  defp put_doctrine(orders, hooks \\ %{}, revision \\ 14) do
    DoctrineMemory.put(@avatar_id, %{
      revision: revision,
      tick: 41_340,
      orders: orders,
      hooks: hooks
    })
  end

  defp edit(socket, target, lines) do
    send_line(socket, "doctrine edit #{target}")
    read_until(socket, "> ")
    Enum.each(lines, &send_line(socket, &1))
    send_line(socket, ".")
    read_until(socket, @prompt)
  end

  test "reports when no doctrine is on file" do
    socket = connect()
    dial(socket, @code)

    send_line(socket, "doctrine")
    assert read_until(socket, @prompt) =~ ~s(NO DOCTRINE ON FILE. "doctrine edit 1")
  end

  test "lists the doctrine on file, clipping long sections" do
    long = String.duplicate("trade the corridor and bank the margin ", 4)
    put_doctrine([long, "fight only when cornered"])

    socket = connect()
    dial(socket, @code)
    send_line(socket, "doctrine")
    listing = read_until(socket, @prompt)

    assert listing =~ "DOCTRINE rev 14 (updated tick 41,340)"
    assert listing =~ "  [1] #{String.slice(long, 0, 66)}...\r\n"
    assert listing =~ "  [2] fight only when cornered\r\n"
  end

  test "an edit saves a draft and nothing transmits until doctrine send" do
    put_doctrine(["trade first", "fight only when cornered"], %{"attacked" => "run away"})

    socket = connect()
    dial(socket, @code)
    send_line(socket, "doctrine edit 2")

    assert read_until(socket, "> ") =~
             ~s(EDITING SECTION 2. End with a single "." on its own line.)

    send_line(socket, "Fight only when cornered, with one exception: the Dial Tone")
    send_line(socket, "Kings. Refuse their tolls.")
    send_line(socket, ".")

    saved = read_until(socket, @prompt)

    assert saved =~
             ~s{DRAFT SAVED: section 2 is 14 words (limit 400). "doctrine send" transmits.}

    assert %{revision: 14, orders: ["trade first" | _]} = DoctrineMemory.doctrines()[@avatar_id]

    send_line(socket, "doctrine send")
    assert read_until(socket, "Submit? (Y/n) ") =~ "18 words total. Submit? (Y/n)"

    send_line(socket, "y")
    assert read_until(socket, @prompt) =~ "TRANSMITTING............ ACCEPTED AS rev 15"

    assert DoctrineMemory.doctrines() == %{
             @avatar_id => %{
               revision: 15,
               tick: 0,
               orders: [
                 "trade first",
                 "Fight only when cornered, with one exception: the Dial Tone Kings. Refuse their tolls."
               ],
               hooks: %{"attacked" => "run away"}
             }
           }
  end

  test "several edits accumulate into a single revision" do
    put_doctrine(["trade first"])

    socket = connect()
    dial(socket, @code)
    edit(socket, "2", ["Build toward a colony on the rock we surveyed."])
    edit(socket, "attacked", ["Refuse the Dial Tone Kings. Fight if pressed."])

    assert %{revision: 14} = DoctrineMemory.doctrines()[@avatar_id]

    send_line(socket, "doctrine send")
    read_until(socket, "Submit? (Y/n) ")
    send_line(socket, "y")
    assert read_until(socket, @prompt) =~ "ACCEPTED AS rev 15"

    assert %{
             revision: 15,
             orders: ["trade first", "Build toward a colony on the rock we surveyed."],
             hooks: %{"attacked" => "Refuse the Dial Tone Kings. Fight if pressed."}
           } = DoctrineMemory.doctrines()[@avatar_id]
  end

  test "doctrine and doctrine hooks show the draft while one exists" do
    put_doctrine(["trade first"], %{"attacked" => "run away"})

    socket = connect()
    dial(socket, @code)
    edit(socket, "1", ["Explore first, trade second."])

    send_line(socket, "doctrine")
    listing = read_until(socket, @prompt)
    assert listing =~ ~s{DOCTRINE DRAFT (unsent; "doctrine send" transmits)}
    assert listing =~ "  [1] Explore first, trade second.\r\n"

    send_line(socket, "doctrine hooks")
    hooks = read_until(socket, @prompt)
    assert hooks =~ "DOCTRINE HOOKS DRAFT (unsent)"
    assert hooks =~ "  [attacked] run away\r\n"
  end

  test "doctrine discard abandons the draft" do
    put_doctrine(["trade first"])

    socket = connect()
    dial(socket, @code)
    edit(socket, "1", ["all new orders"])

    send_line(socket, "doctrine discard")
    assert read_until(socket, @prompt) =~ "DRAFT DISCARDED."

    send_line(socket, "doctrine")
    listing = read_until(socket, @prompt)
    assert listing =~ "DOCTRINE rev 14"
    assert listing =~ "  [1] trade first\r\n"

    send_line(socket, "doctrine send")
    assert read_until(socket, @prompt) =~ ~s(NO DRAFT TO SEND. "doctrine edit ..." starts one.)

    send_line(socket, "doctrine discard")
    assert read_until(socket, @prompt) =~ "NO DRAFT."
  end

  test "n at the send prompt keeps the draft" do
    put_doctrine(["trade first"])

    socket = connect()
    dial(socket, @code)
    edit(socket, "1", ["all new orders"])

    send_line(socket, "doctrine send")
    read_until(socket, "Submit? (Y/n) ")
    send_line(socket, "n")
    assert read_until(socket, @prompt) =~ "NOTHING SENT. DRAFT KEPT."
    assert %{revision: 14, orders: ["trade first"]} = DoctrineMemory.doctrines()[@avatar_id]

    send_line(socket, "doctrine send")
    read_until(socket, "Submit? (Y/n) ")
    send_line(socket, "")
    assert read_until(socket, @prompt) =~ "ACCEPTED AS rev 15"
    assert %{revision: 15, orders: ["all new orders"]} = DoctrineMemory.doctrines()[@avatar_id]
  end

  test "writes the first doctrine when none is on file" do
    socket = connect()
    dial(socket, @code)
    send_line(socket, "doctrine edit 1")
    assert read_until(socket, "> ") =~ "EDITING NEW SECTION 1."

    send_line(socket, "I'm a trader first.")
    send_line(socket, ".")
    read_until(socket, @prompt)

    send_line(socket, "doctrine send")
    read_until(socket, "Submit? (Y/n) ")
    send_line(socket, "y")

    assert read_until(socket, @prompt) =~ "ACCEPTED AS rev 1"
    assert %{orders: ["I'm a trader first."]} = DoctrineMemory.doctrines()[@avatar_id]
  end

  test "rejects a section number out of range and an unknown hook" do
    put_doctrine(["trade first"])

    socket = connect()
    dial(socket, @code)

    for target <- ["7", "tolled"] do
      send_line(socket, "doctrine edit #{target}")
      assert read_until(socket, @prompt) =~ "SECTION MUST BE 1..2 OR A HOOK (attacked, moved)."
    end
  end

  test "doctrine hooks lists set and unset hooks" do
    put_doctrine(["trade first"], %{"attacked" => "refuse tolls and fight"})

    socket = connect()
    dial(socket, @code)
    send_line(socket, "doctrine hooks")
    listing = read_until(socket, @prompt)

    assert listing =~ "DOCTRINE HOOKS rev 14"
    assert listing =~ "  [attacked] refuse tolls and fight\r\n"
    assert listing =~ "  [moved]    (not set)\r\n"
  end

  test "doctrine hooks with no doctrine enumerates the available hooks" do
    socket = connect()
    dial(socket, @code)
    send_line(socket, "doctrine hooks")

    assert read_until(socket, @prompt) =~ "NO DOCTRINE ON FILE. HOOKS AVAILABLE: attacked, moved."
  end

  test "clears a hook with an empty body" do
    put_doctrine(["trade first"], %{"attacked" => "run away"})

    socket = connect()
    dial(socket, @code)
    send_line(socket, "doctrine edit attacked")
    read_until(socket, "> ")
    send_line(socket, ".")

    saved = read_until(socket, @prompt)
    assert saved =~ ~s(DRAFT SAVED: hook attacked cleared. "doctrine send" transmits.)

    send_line(socket, "doctrine send")
    read_until(socket, "Submit? (Y/n) ")
    send_line(socket, "y")
    assert read_until(socket, @prompt) =~ "ACCEPTED AS rev 15"
    assert %{orders: ["trade first"], hooks: %{}} = DoctrineMemory.doctrines()[@avatar_id]
  end

  test "an empty body on an unset hook saves nothing" do
    put_doctrine(["trade first"])

    socket = connect()
    dial(socket, @code)
    send_line(socket, "doctrine edit moved")
    read_until(socket, "> ")
    send_line(socket, ".")

    assert read_until(socket, @prompt) =~ "NOTHING SAVED."

    send_line(socket, "doctrine send")
    assert read_until(socket, @prompt) =~ "NO DRAFT TO SEND."
  end

  test "hook editing needs standing orders first" do
    socket = connect()
    dial(socket, @code)
    send_line(socket, "doctrine edit attacked")

    assert read_until(socket, @prompt) =~
             ~s(HOOKS NEED STANDING ORDERS. "doctrine edit 1" writes the first section.)
  end

  test "a rejected send keeps the draft" do
    put_doctrine(["trade first"])

    socket = connect()
    dial(socket, @code)
    edit(socket, "1", ["too many words"])

    DoctrineMemory.fail_with({:invalid, "412 words, limit is 400"})
    send_line(socket, "doctrine send")
    read_until(socket, "Submit? (Y/n) ")
    send_line(socket, "y")

    rejected = read_until(socket, @prompt)
    assert rejected =~ "REJECTED: 412 words, limit is 400"
    assert rejected =~ "DRAFT KEPT."

    send_line(socket, "doctrine")
    assert read_until(socket, @prompt) =~ "DOCTRINE DRAFT"
  end

  test "prints news to the dialed-in session and restores the prompt" do
    socket = connect()
    dial(socket, @code)

    News.broadcast(@avatar_id, %{tick: 41_884, text: "DOCTRINE rev 15 RECEIVED"})

    arrival = read_until(socket, @prompt)
    assert arrival =~ "  41,884  DOCTRINE rev 15 RECEIVED\r\n"
  end
end
