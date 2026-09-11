defmodule PlugStatem.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/jessestimpson/plug_statem"

  def project do
    [
      app: :plug_statem,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      description: description(),
      package: package(),
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: []
    ]
  end

  defp deps do
    [
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:dialyxir, "~> 1.4", only: :dev, runtime: false},
      # Integration tests run the loop inside a real Bandit request.
      {:bandit, "~> 1.12", only: :test}
    ]
  end

  defp aliases do
    [
      lint: [
        "format --check-formatted",
        "deps.unlock --check-unused",
        "dialyzer",
        "docs --warnings-as-errors"
      ]
    ]
  end

  defp description do
    "A gen_statem-style event loop that fits the Plug contract."
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url}
    ]
  end

  defp docs do
    [
      main: "PlugStatem",
      source_url: @source_url,
      source_ref: "v#{@version}"
    ]
  end
end
