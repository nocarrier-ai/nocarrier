defmodule NoCarrier.Doctrine.Service do
  @type entry :: %{
          revision: integer(),
          tick: integer(),
          orders: [String.t()],
          hooks: %{String.t() => String.t()}
        }

  @type receipt :: %{revision: integer(), tick: integer(), hash: String.t()}

  @callback doctrine(String.t()) :: {:ok, entry()} | :not_found | {:error, term()}
  @callback update(String.t(), [String.t()], %{String.t() => String.t()}) ::
              {:ok, receipt()} | {:error, term()}
end
