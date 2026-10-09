import Config

if config_env() == :dev do
  dotenv = Path.expand("../.env", __DIR__)

  if File.exists?(dotenv) do
    for line <- File.stream!(dotenv),
        line = String.trim(line),
        line != "",
        not String.starts_with?(line, "#"),
        [key, value] <- [String.split(String.replace_prefix(line, "export ", ""), "=", parts: 2)],
        is_nil(System.get_env(key)) do
      System.put_env(key, value |> String.trim("\"") |> String.trim("'"))
    end
  end
end

if System.get_env("PHX_SERVER") do
  config :no_carrier, NoCarrierWeb.Endpoint, server: true
end

config :no_carrier, NoCarrierWeb.Endpoint,
  http: [port: String.to_integer(System.get_env("PORT", "4000"))]

if config_env() != :test do
  config :ueberauth, Ueberauth.Strategy.Github.OAuth,
    client_id: {:system, "GITHUB_CLIENT_ID"},
    client_secret: {:system, "GITHUB_CLIENT_SECRET"}
end

if fly_app_name = System.get_env("FLY_APP_NAME") do
  config :libcluster,
    topologies: [
      fly6pn: [
        strategy: Cluster.Strategy.DNSPoll,
        config: [
          polling_interval: 5_000,
          query: "#{fly_app_name}.internal",
          node_basename: fly_app_name
        ]
      ]
    ]
end

if config_env() == :dev do
  if Node.alive?() do
    config :libcluster, topologies: [local_epmd: [strategy: Cluster.Strategy.LocalEpmd]]
  end

  config :no_carrier, NoCarrierWeb.Endpoint,
    live_reload: [
      web_console_logger: true,
      patterns: [
        ~r"priv/static/(?!uploads/).*\.(js|css|png|jpeg|jpg|gif|svg)$"E,
        ~r"priv/gettext/.*\.po$"E,
        ~r"lib/no_carrier_web/router\.ex$"E,
        ~r"lib/no_carrier_web/(controllers|live|components)/.*\.(ex|heex)$"E
      ]
    ]
end

if config_env() == :prod do
  secret_key_base =
    System.get_env("SECRET_KEY_BASE") ||
      raise """
      environment variable SECRET_KEY_BASE is missing.
      You can generate one by calling: mix phx.gen.secret
      """

  for var <- ["GITHUB_CLIENT_ID", "GITHUB_CLIENT_SECRET"], is_nil(System.get_env(var)) do
    raise """
    environment variable #{var} is missing.
    Create a GitHub OAuth app and set its client ID and client secret.
    """
  end

  host = System.get_env("PHX_HOST") || "example.com"

  config :no_carrier, NoCarrierWeb.Endpoint,
    url: [host: host, port: 443, scheme: "https"],
    http: [ip: {0, 0, 0, 0, 0, 0, 0, 0}],
    secret_key_base: secret_key_base
end
