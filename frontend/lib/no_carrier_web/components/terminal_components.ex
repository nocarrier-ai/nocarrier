defmodule NoCarrierWeb.TerminalComponents do
  use Phoenix.Component

  attr :id, :string, default: nil
  attr :title, :string, required: true
  attr :class, :string, default: nil
  slot :inner_block, required: true

  def panel(assigns) do
    ~H"""
    <section id={@id} class={["panel", @class]}>
      <h2 class="panel-title">{@title}</h2>
      {render_slot(@inner_block)}
    </section>
    """
  end

  attr :class, :string, default: nil
  slot :inner_block, required: true

  def prompt(assigns) do
    ~H"""
    <p class={["prompt", @class]}>{render_slot(@inner_block)}</p>
    """
  end
end
