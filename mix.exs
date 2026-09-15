defmodule XSockets.MixProject do
  use Mix.Project

  def project do
    [
      app: :xsockets,
      version: "1.0.0",
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description:
        "Format-agnostic socket library with pluggable packet framing, a reusable drain engine, and multi-protocol transport support.",
      source_url: "https://github.com/Lazarus404/xsockets",
      homepage_url: "https://github.com/Lazarus404/xsockets",
      package: package(),
      docs: [
        extras: ["README.md", "LICENSE.md", "CHANGELOG.md", "ARCHITECTURE.md"],
        main: "readme"
      ],
      dialyzer: [
        plt_add_apps: [:ex_sctp],
        flags: [:error_handling, :underspecs]
      ]
    ]
  end

  def cli do
    [preferred_envs: [dialyzer: :dev]]
  end

  def application do
    [
      extra_applications: [:logger, :ssl],
      mod: {XSockets.Application, []}
    ]
  end

  defp deps do
    [
      {:telemetry, "~> 1.0"},
      {:ex_sctp, "~> 0.1", optional: true},
      {:benchee, "~> 1.3", only: :dev},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp package do
    %{
      files: [
        "lib",
        "mix.exs",
        "README.md",
        "LICENSE.md",
        "CHANGELOG.md",
        "ARCHITECTURE.md",
        "config",
        "examples"
      ],
      maintainers: ["Jahred Love"],
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => "https://github.com/Lazarus404/xsockets"}
    }
  end
end
