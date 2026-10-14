defmodule NoCarrier.NewsTest do
  use ExUnit.Case, async: true

  alias NoCarrier.News

  test "the listener rebroadcasts a news item to the player's topic" do
    News.subscribe("p1")

    News.Listener.handle_message(%{
      topic: "news.p1",
      body: ~s({"player_id":"p1","tick":41884,"text":"DOCTRINE rev 15 RECEIVED"})
    })

    assert_receive {:news, %{tick: 41_884, text: "DOCTRINE rev 15 RECEIVED"}}
  end

  test "news for another player does not arrive" do
    News.subscribe("p1")

    News.Listener.handle_message(%{topic: "news.p2", body: ~s({"tick":1,"text":"hi"})})

    refute_receive {:news, _item}
  end

  @tag :capture_log
  test "a malformed news item is dropped" do
    News.subscribe("p1")

    News.Listener.handle_message(%{topic: "news.p1", body: "{"})
    News.Listener.handle_message(%{topic: "news.p1", body: ~s({"tick":"soon","text":"hi"})})

    refute_receive {:news, _item}
  end

  test "the listener starts nothing when NATS is not configured" do
    assert News.Listener.start_link() == :ignore
  end
end
