
defmodule MyXQLEx.EctoAdapter do
    require Logger
    use Ecto.Adapters.SQL,
      driver: :myxqlex,
      migration_lock: nil

    @impl true
    def supports_ddl_transaction?, do: false

    @impl true
    def loaders({:embed, _} = type, _), do: [&json_decode/1, &Ecto.Adapters.SQL.load_embed(type, &1)]
    def loaders({:map, _}, type),       do: [&json_decode/1, &Ecto.Adapters.SQL.load_embed(type, &1)]
    def loaders(:map, type),            do: [&json_decode/1, type]
    def loaders(:float, type),          do: [&float_decode/1, type]
    def loaders(:boolean, type),        do: [&bool_decode/1, type]
    def loaders(:string, type),         do: [&string_decode/1, type]
    def loaders(:naive_datetime, type), do: [&naive_datetime_decode/1, type]
    def loaders(:binary_id, type),      do: [Ecto.UUID, type]
    def loaders(_, type),               do: [type]

    defp bool_decode(<<0>>), do: {:ok, false}
    defp bool_decode(<<1>>), do: {:ok, true}
    defp bool_decode(0), do: {:ok, false}
    defp bool_decode(1), do: {:ok, true}
    defp bool_decode(x), do: {:ok, x}

    defp float_decode(%Decimal{} = decimal), do: {:ok, Decimal.to_float(decimal)}
    defp float_decode(x), do: {:ok, x}

    defp string_decode(null), do: {:ok, nil}
    defp string_decode(x), do: {:ok, x}

    defp json_decode(x) when is_binary(x), do: {:ok, MyXQLEx.json_library().decode!(x)}
    defp json_decode(x), do: {:ok, x}
    defp naive_datetime_decode({{y, m, d}, {h, min, s}} = x), do: NaiveDateTime.new(y, m, d, h, min, s)
    defp naive_datetime_decode(x), do: {:ok, x}

    @impl true
    def storage_up(opts) do
      database = Keyword.fetch!(opts, :database) || raise ":database is nil in repository configuration"
      opts = Keyword.delete(opts, :database)
      charset = opts[:charset] || "utf8"

      command =
        ~s(CREATE DATABASE `#{database}` DEFAULT CHARACTER SET = #{charset})
        |> concat_if(opts[:collation], &"DEFAULT COLLATE = #{&1}")

      case run_query(command, opts) do
        {:ok, _} ->
          :ok
        {:error, %{mysql: %{name: :ER_DB_CREATE_EXISTS}}} ->
          {:error, :already_up}
        {:error, error} ->
          {:error, Exception.message(error)}
        {:exit, exit} ->
          {:error, exit_to_exception(exit)}
      end
    end

    defp concat_if(content, nil, _fun),  do: content
    defp concat_if(content, value, fun), do: content <> " " <> fun.(value)

    @impl true
    def storage_down(opts) do
      database = Keyword.fetch!(opts, :database) || raise ":database is nil in repository configuration"
      opts = Keyword.delete(opts, :database)
      command = "DROP DATABASE `#{database}`"

      case run_query(command, opts) do
        {:ok, _} ->
          :ok
        {:error, %{mysql: %{name: :ER_DB_DROP_EXISTS}}} ->
          {:error, :already_down}
        {:error, %{mysql: %{name: :ER_BAD_DB_ERROR}}} ->
          {:error, :already_down}
        {:exit, :killed} ->
          {:error, :already_down}
        {:exit, exit} ->
          {:error, exit_to_exception(exit)}
      end
    end

    @impl true
    def supports_ddl_transaction? do
      false
    end

    @impl true
    def insert(adapter_meta, schema_meta, params, on_conflict, returning, opts) do
      %{source: source, prefix: prefix} = schema_meta
      {_, query_params, _} = on_conflict

      key = primary_key!(schema_meta, returning)
      {fields, values} = :lists.unzip(params)
      sql = @conn.insert(prefix, source, fields, [fields], on_conflict, [])

      cache_statement = "ecto_insert_#{source}"
      opts = [{:cache_statement, cache_statement} | opts]

      case Ecto.Adapters.SQL.query(adapter_meta, sql, values ++ query_params, opts) do
        {:ok, %{num_rows: 1, last_insert_id: last_insert_id}} ->
          {:ok, last_insert_id(key, last_insert_id)}

        {:ok, %{num_rows: 2, last_insert_id: last_insert_id}} ->
          {:ok, last_insert_id(key, last_insert_id)}

        {:error, err} ->
          case @conn.to_constraints(err) do
            []          -> raise err
            constraints -> {:invalid, constraints}
          end
      end
    end

    defp primary_key!(%{autogenerate_id: {_, key, _type}}, [key]), do: key
    defp primary_key!(_, []), do: nil
    defp primary_key!(%{schema: schema}, returning) do
      raise ArgumentError, "MySQL does not support :read_after_writes in schemas for non-primary keys. " <>
                          "The following fields in #{inspect schema} are tagged as such: #{inspect returning}"
    end

    defp last_insert_id(nil, _last_insert_id), do: []
    defp last_insert_id(_key, 0), do: []
    defp last_insert_id(key, last_insert_id), do: [{key, last_insert_id}]

    @impl true
    def structure_dump(default, config) do
      table = config[:migration_source] || "schema_migrations"
      path  = config[:dump_path] || Path.join(default, "structure.sql")

      with {:ok, versions} <- select_versions(table, config),
          {:ok, contents} <- mysql_dump(config),
          {:ok, contents} <- append_versions(table, versions, contents) do
        File.mkdir_p!(Path.dirname(path))
        File.write!(path, contents)
        {:ok, path}
      end
    end

    defp select_versions(table, config) do
      case run_query(~s[SELECT version FROM `#{table}` ORDER BY version], config) do
        {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, &hd/1)}
        {:error, %{mysql: %{name: :ER_NO_SUCH_TABLE}}} -> {:ok, []}
        {:error, _} = error -> error
        {:exit, exit} -> {:error, exit_to_exception(exit)}
      end
    end

    defp mysql_dump(config) do
      case run_with_cmd("mysqldump", config, ["--no-data", "--routines", config[:database]]) do
        {output, 0} -> {:ok, output}
        {output, _} -> {:error, output}
      end
    end

    defp append_versions(_table, [], contents) do
      {:ok, contents}
    end
    defp append_versions(table, versions, contents) do
      {:ok,
        contents <>
        ~s[INSERT INTO `#{table}` (version) VALUES ] <>
        Enum.map_join(versions, ", ", &"(#{&1})") <>
        ~s[;\n\n]}
    end

    @impl true
    def structure_load(default, config) do
      path = config[:dump_path] || Path.join(default, "structure.sql")

      args = [
        "--execute", "SET FOREIGN_KEY_CHECKS = 0; SOURCE #{path}; SET FOREIGN_KEY_CHECKS = 1",
        "--database", config[:database]
      ]

      case run_with_cmd("mysql", config, args) do
        {_output, 0} -> {:ok, path}
        {output, _}  -> {:error, output}
      end
    end

    ## Helpers

    defp run_query(sql, opts) do
      {:ok, _} = Application.ensure_all_started(:myxqlex)

      opts =
        opts
        |> Keyword.drop([:name, :log, :pool, :pool_size])
        |> Keyword.put(:backoff_type, :stop)
        |> Keyword.put(:max_restarts, 0)

      {:ok, pid} = Task.Supervisor.start_link

      task = Task.Supervisor.async_nolink(pid, fn ->
        {:ok, conn} = MyXQLEx.start_link(opts)

        value = MyXQLEx.query(conn, sql, [], opts)
        GenServer.stop(conn)
        value
      end)

      timeout = Keyword.get(opts, :timeout, 15_000)

      case Task.yield(task, timeout) || Task.shutdown(task) do
        {:ok, {:ok, result}} ->
          {:ok, result}
        {:ok, {:error, error}} ->
          {:error, error}
        {:exit, exit} ->
          {:exit, exit}
        nil ->
          {:error, RuntimeError.exception("command timed out")}
      end
    end

    defp exit_to_exception({%{__struct__: struct} = error, _})
        when struct in [MyXQLEx.Error, DBConnection.Error],
        do: error

    defp exit_to_exception(reason), do: RuntimeError.exception(Exception.format_exit(reason))

    defp run_with_cmd(cmd, opts, opt_args) do
      unless System.find_executable(cmd) do
        raise "could not find executable `#{cmd}` in path, " <>
              "please guarantee it is available before running ecto commands"
      end

      env =
        if password = opts[:password] do
          [{"MYSQL_PWD", password}]
        else
          []
        end

      host     = opts[:hostname] || System.get_env("MYSQL_HOST") || "localhost"
      port     = opts[:port] || System.get_env("MYSQL_TCP_PORT") || "3306"
      protocol = opts[:cli_protocol] || System.get_env("MYSQL_CLI_PROTOCOL") || "tcp"

      user_args =
        if username = opts[:username] do
          ["--user", username]
        else
          []
        end

      args =
        [
          "--host", host,
          "--port", to_string(port),
          "--protocol", protocol
        ] ++ user_args ++ opt_args

      System.cmd(cmd, args, env: env, stderr_to_stdout: true)
    end

end