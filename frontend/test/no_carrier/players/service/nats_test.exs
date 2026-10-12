defmodule NoCarrier.Players.Service.NatsTest do
  use ExUnit.Case, async: true

  alias NoCarrier.Players.Service.Nats

  @created ~s({"player_id":"p1","display_name":"Ada","dial_in_code":"7719-0402-5518","tick":7})

  test "decodes a created reply" do
    assert Nats.decode_reply(%{body: @created}) ==
             {:ok, %{player_id: "p1", display_name: "Ada", dial_in_code: "7719-0402-5518"}}
  end

  test "turns a 400 into an invalid error naming the reason" do
    reply = %{
      body: "",
      headers: [
        {"Nats-Service-Error-Code", "400"},
        {"Nats-Service-Error", "missing display name"}
      ]
    }

    assert Nats.decode_reply(reply) == {:error, {:invalid, "missing display name"}}
  end

  test "turns a 409 into already registered" do
    reply = %{
      body: "",
      headers: [
        {"nats-service-error-code", "409"},
        {"nats-service-error", "player already registered for that oauth id"}
      ]
    }

    assert Nats.decode_reply(reply) == {:error, :already_registered}
  end

  test "reports other service errors with their code" do
    reply = %{
      body: "",
      headers: [{"Nats-Service-Error-Code", "500"}, {"Nats-Service-Error", "boom"}]
    }

    assert Nats.decode_reply(reply) == {:error, {:rejected, "500", "boom"}}
  end

  test "rejects a reply it cannot read" do
    assert Nats.decode_reply(%{body: "{"}) == {:error, :malformed_reply}
    assert Nats.decode_reply(%{body: ~s({"player_id":"p1"})}) == {:error, :malformed_reply}
  end
end
