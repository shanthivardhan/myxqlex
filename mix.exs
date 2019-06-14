defmodule MyXQLEx.MixProject do
  use Mix.Project

  def project do
    [
      app: :myxqlex,
      version: "0.1.0",
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"},
      {:ecto_sql, "~> 3.0"},
      {:db_connection, "~> 2.0", db_connection_opts()},
      {:mysql, "~> 1.5.0"},
      {:jason, "~> 1.0", optional: true},
    ]
  end

  defp db_connection_opts() do
    if path = System.get_env("DB_CONNECTION_PATH") do
      [path: path]
    else
      []
    end
  end

end
