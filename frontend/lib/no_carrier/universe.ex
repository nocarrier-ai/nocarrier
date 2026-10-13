defmodule NoCarrier.Universe do
  alias NoCarrier.Nats

  @status_subject "query.universe.status"

  def status do
    with {:ok, %{body: body}} <- Nats.request(@status_subject, ""),
         {:ok, %{} = status} <- Jason.decode(body) do
      {:ok, status}
    else
      {:ok, _other} -> {:error, :malformed_reply}
      {:error, _reason} = error -> error
    end
  end
end
