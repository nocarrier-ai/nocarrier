defmodule NoCarrierWeb.PageController do
  use NoCarrierWeb, :controller

  def home(conn, _params) do
    render(conn, :home)
  end
end
