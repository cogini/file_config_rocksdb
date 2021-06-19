defmodule FileConfigRocksdb.MixProject do
  use Mix.Project

  @github "https://github.com/cogini/file_config_rocksdb"

  def project do
    [
      app: :file_config_rocksdb,
      version: "0.1.0",
      elixir: "~> 1.11",
      start_permanent: Mix.env() == :prod,
      description: description(),
      package: package(),
      docs: docs(),
      deps: deps(),
      source_url: @github,
      homepage_url: @github,
      dialyzer: [
        plt_add_apps: [:mix]
        # plt_add_deps: true,
        # flags: ["-Werror_handling", "-Wrace_conditions"],
        # flags: ["-Wunmatched_returns", :error_handling, :race_conditions, :underspecs],
        # ignore_warnings: "dialyzer.ignore-warnings"
      ],
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test
      ]
      # xref: [
      #   exclude: [EEx, :cover]
      # ]
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {FileConfigRocksdb.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:credo, "~> 1.5", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.0", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.23", only: :dev, runtime: false},
      {:excoveralls, "~> 0.14.0", only: [:dev, :test], runtime: false},
      {:file_config, github: "cogini/file_config"},
      {:nimble_csv, "~> 0.3"},
      # {:rocksdb, git: "https://gitlab.com/barrel-db/erlang-rocksdb.git"},
      {:rocksdb, "~> 1.6"},
    ]
  end

  defp description do
    "FileConfig storage module for rocksdb."
  end

  defp package do
    [
      maintainers: ["Jake Morrison"],
      licenses: ["Apache 2.0"],
      links: %{"GitHub" => @github}
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "CHANGELOG.md"],
      skip_undefined_reference_warnings_on: ["CHANGELOG.md"],
      source_url: @github,
      # api_reference: false,
      source_url_pattern: "#{@github}/blob/master/%{path}#L%{line}"
    ]
  end
end
