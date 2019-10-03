defmodule FileConfigRocksdb.MixProject do
  use Mix.Project

  def project do
    [
      app: :file_config_rocksdb,
      version: "0.1.0",
      elixir: "~> 1.8",
      start_permanent: Mix.env() == :prod,
      dialyzer: [
        # plt_add_deps: :project,
        # plt_add_apps: [:ssl, :mnesia, :compiler, :xmerl, :inets, :disk_log],
        plt_add_deps: true,
        # flags: ["-Werror_handling", "-Wrace_conditions"],
        # flags: ["-Wunmatched_returns", :error_handling, :race_conditions, :underspecs],
        # ignore_warnings: "dialyzer.ignore-warnings"
      ],
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :lager]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:dialyxir, "~> 0.5.1", only: [:dev, :test], runtime: false},
      {:exlager, github: "khia/exlager"},
      {:lager, "~> 3.6", override: true},
      {:nimble_csv, "~> 0.3"},
      # {:rocksdb, git: "https://gitlab.com/barrel-db/erlang-rocksdb.git"},
      {:rocksdb, "~> 1.3"},
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end
end
