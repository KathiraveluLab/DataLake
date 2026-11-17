defmodule DataLake.MixProject do
  use Mix.Project

  def project do
    [
      app: :data_lake,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {DataLake.Application, []}
    ]
  end

  defp deps do
    []
  end
end

