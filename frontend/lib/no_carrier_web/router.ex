defmodule NoCarrierWeb.Router do
  use NoCarrierWeb, :router

  import NoCarrierWeb.PlayerAuth

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {NoCarrierWeb.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
    plug :fetch_current_scope_for_player
  end

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/", NoCarrierWeb do
    pipe_through :browser

    get "/", PageController, :home
    delete "/log-out", AuthController, :delete
  end

  scope "/auth", NoCarrierWeb do
    pipe_through :browser

    get "/:provider", AuthController, :request
    get "/:provider/callback", AuthController, :callback
  end

  scope "/", NoCarrierWeb do
    pipe_through [:browser, :require_authenticated_player]

    live_session :require_authenticated_player,
      on_mount: [{NoCarrierWeb.PlayerAuth, :ensure_authenticated}] do
      live "/play", PlayLive
    end
  end

  if Application.compile_env(:no_carrier, :dev_routes) do
    import Phoenix.LiveDashboard.Router

    scope "/dev" do
      pipe_through :browser

      live_dashboard "/dashboard", metrics: NoCarrierWeb.Telemetry
    end
  end
end
