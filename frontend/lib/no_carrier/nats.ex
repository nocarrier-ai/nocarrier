defmodule NoCarrier.Nats do
  @connection NoCarrier.Nats.Connection
  @request_timeout 5_000

  def child_spec(_opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, []}}
  end

  def start_link do
    case Application.get_env(:no_carrier, __MODULE__) do
      nil ->
        :ignore

      config ->
        Gnat.ConnectionSupervisor.start_link(%{
          name: @connection,
          backoff_period: 2_000,
          connection_settings: [connection_settings(config)]
        })
    end
  end

  def request(subject, body) do
    Gnat.request(@connection, subject, body, receive_timeout: @request_timeout)
  catch
    :exit, _ -> {:error, :unavailable}
  end

  def kv_get(bucket, key) do
    case Gnat.Jetstream.API.KV.get_value(@connection, bucket, key) do
      value when is_binary(value) -> {:ok, value}
      nil -> :not_found
      {:error, %{"code" => 404}} -> :not_found
      {:error, reason} -> {:error, reason}
    end
  catch
    :exit, _ -> {:error, :unavailable}
  end

  def connection_settings(config) do
    uri = URI.parse(Keyword.fetch!(config, :url))

    %{host: uri.host, port: uri.port || 4222, tls: uri.scheme == "tls", no_responders: true}
    |> put_creds(Keyword.get(config, :creds))
  end

  defp put_creds(settings, nil), do: settings
  defp put_creds(settings, ""), do: settings

  defp put_creds(settings, path) do
    creds = File.read!(path)

    Map.merge(settings, %{
      jwt: creds_section(creds, "NATS USER JWT"),
      nkey_seed: creds_section(creds, "USER NKEY SEED")
    })
  end

  defp creds_section(creds, label) do
    case Regex.run(~r/-----BEGIN #{label}-----\s*(\S+)\s*-+END/, creds) do
      [_, value] -> value
      nil -> raise ArgumentError, "NATS credentials file has no #{label} section"
    end
  end
end
