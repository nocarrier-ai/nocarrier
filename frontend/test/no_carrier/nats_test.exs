defmodule NoCarrier.NatsTest do
  use ExUnit.Case, async: true

  alias NoCarrier.Nats

  @creds """
  -----BEGIN NATS USER JWT-----
  eyJ0eXAiOiJKV1QifQ.eyJzdWIiOiJ0ZXN0In0.sig
  ------END NATS USER JWT------

  ************************* IMPORTANT *************************
  NKEY Seed printed below can be used to sign and prove identity.

  -----BEGIN USER NKEY SEED-----
  SUAEXAMPLESEEDXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
  ------END USER NKEY SEED------

  *************************************************************
  """

  test "parses a plain url" do
    assert Nats.connection_settings(url: "nats://nats.internal:4222") == %{
             host: "nats.internal",
             port: 4222,
             tls: false,
             no_responders: true
           }
  end

  test "defaults the port and recognises tls" do
    assert %{host: "connect.ngs.global", port: 4222, tls: true} =
             Nats.connection_settings(url: "tls://connect.ngs.global")
  end

  test "reads a credentials file" do
    path = Path.join(System.tmp_dir!(), "nocarrier-#{System.unique_integer([:positive])}.creds")
    File.write!(path, @creds)
    on_exit(fn -> File.rm(path) end)

    assert %{jwt: "eyJ0eXAiOiJKV1QifQ.eyJzdWIiOiJ0ZXN0In0.sig", nkey_seed: "SUAEXAMPLESEED" <> _} =
             Nats.connection_settings(url: "tls://connect.ngs.global", creds: path)
  end

  test "starts nothing when no url is configured" do
    assert Nats.start_link() == :ignore
  end
end
