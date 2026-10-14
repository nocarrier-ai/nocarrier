defmodule NoCarrier.DoctrineTest do
  use ExUnit.Case, async: true

  alias NoCarrier.Doctrine

  test "counts words across orders and hooks like the relay does" do
    orders = ["trade first", "  fight   only when cornered "]
    hooks = %{"attacked" => "run away"}

    assert Doctrine.word_count(orders, hooks) == 8
    assert Doctrine.word_count([], %{}) == 0
  end

  test "counts the words of one section or hook" do
    assert Doctrine.word_count("  fight   only when cornered ") == 4
    assert Doctrine.word_count("") == 0
  end

  test "the hook roster matches the relay's" do
    assert Doctrine.hooks() == ["attacked", "moved"]
  end

  describe "Service.Nats.decode_reply/1" do
    test "decodes an accepted update" do
      reply = %{body: ~s({"revision":15,"tick":41882,"hash":"abc"})}

      assert Doctrine.Service.Nats.decode_reply(reply) ==
               {:ok, %{revision: 15, tick: 41_882, hash: "abc"}}
    end

    test "maps the service error codes" do
      assert {:error, {:invalid, "too long"}} =
               Doctrine.Service.Nats.decode_reply(%{
                 body: "",
                 headers: [{"nats-service-error-code", "400"}, {"nats-service-error", "too long"}]
               })

      assert {:error, :conflict} =
               Doctrine.Service.Nats.decode_reply(%{
                 body: "",
                 headers: [{"nats-service-error-code", "409"}]
               })

      assert {:error, {:rejected, "500", _message}} =
               Doctrine.Service.Nats.decode_reply(%{
                 body: "",
                 headers: [{"nats-service-error-code", "500"}]
               })
    end

    test "rejects a malformed accepted body" do
      assert Doctrine.Service.Nats.decode_reply(%{body: ~s({"revision":"x"})}) ==
               {:error, :malformed_reply}
    end
  end
end
