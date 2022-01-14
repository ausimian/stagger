defmodule Stagger.MixProject do
  use Mix.Project

  @version "0.1.7"

  def project do
    [
      app: :stagger,
      version: @version,
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      name: "Stagger",
      description: "Point-to-point, durable message-queues",
      source_url: "https://github.com/ausimian/stagger",
      docs: docs(),
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test
      ],
      package: package()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Stagger.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:gen_stage,   "~> 1.1"},
      {:ex_doc,      "~> 0.27", only: [:dev], runtime: false},
      {:propcheck,   "~> 1.4",  only: [:test, :dev]},
      {:excoveralls, "~> 0.10", only: [:test]}
    ]
  end

  defp docs() do
    [
      main: "Stagger",
      source_ref: "v#{@version}"
    ]
  end

  defp package() do
    [
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE.md CHANGELOG.md),
      exclude_patterns: ~w(lib/mix test),
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/ausimian/stagger"}
    ]
  end

end
