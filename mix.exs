defmodule Kvstore.MixProject do
    use Mix.Project

    def project do
      [
        app: :kv_store,
        version: "0.1.0",
        build_path: "./_build",
        config_path: "./config/config.exs",
        deps_path: "../../deps",
        lockfile: "./mix.lock",
        elixir: "~> 1.10",
        start_permanent: Mix.env() == :prod,
        deps: deps(),
        dializer: [
          plt_add_deps: :apps_direct
        ],
        docs: [
            # The main page in the docs
            main: "KvStore"
          ],
      ]
    end

    # Run "mix help compile.app" to learn about applications.
    def application do
      [
        mod: {DistributedKVStore.Application, []},
        extra_applications: [:logger, :libcluster]
      ]
    end

    # Run "mix help deps" to learn about dependencies.
    defp deps do
      [
        {:dialyxir, "~> 1.0", only: [:dev], runtime: false},
        {:credo, "~> 1.4", only: [:dev, :test], runtime: false},
        {:statistics, "~> 0.6.2"},
        {:ex_doc, "~> 0.22", only: :dev, runtime: false},
        {:local_cluster, "~> 1.2", only: :test},
        {:mox, "~> 1.0", only: :test},
        {:libcluster, "~> 3.3"}
      ]
    end
  end
