defmodule MyXQLEx.Proxy do
    use GenServer
    require Logger
    @timeout 5000

    defmacrop raiser(result) do
        quote do
          case unquote(result) do
            {:error, error} ->
              raise error
            result ->
              result
          end
        end
      end

    defp get_command(statement) when is_binary(statement) do
    statement |> String.split(" ", parts: 2) |> hd |> String.downcase |> String.to_atom
    end
    defp get_command(nil), do: nil

    defp opts_convert_to_char_list(k) do
    Enum.map(k,
        fn(x) ->
        if is_binary(elem(x,1)) do
            { elem(x,0), String.to_charlist(elem(x,1))}
        else
            x
        end
        end
    )
    end

    def start_link(opts) do
      keys = [:host, :username, :password, :database, :port, :timeout]
      hostname = Keyword.get(opts, :hostname)
      normalized_opts =
      (opts ++ [host: hostname])
      |> Keyword.take(keys)
      |> Enum.map(&normalize_opt/1)

      GenServer.start_link(__MODULE__, normalized_opts)
    end

    def stop(pid) do
      GenServer.stop(pid, :normal)
    end

    defp normalize_opt({k, v}) when is_binary(v) do
      {k, String.to_charlist(v)}
    end

    defp normalize_opt(other), do: other

    @spec query(pid, iodata, list, Keyword.t) :: {:ok, MyXQLEx.Result.t} | {:error, MyXQLEx.Error.t}
    def query(pid, statement, params \\ [], opts \\ []) do
      GenServer.call(pid, {:query, statement, params, opts})
    end

    @impl true
    def init(opts) do
      # sock_type = (opts[:sock_type] || :tcp) |> Atom.to_string |> String.capitalize()
      # sock_mod = ("Elixir.MyXQLEx.Connection." <> sock_type) |> String.to_atom
      queries = ( Keyword.get(opts, :queries) || [] ) ++ ["SET CHARACTER SET " <> (opts[:charset] || "utf8")]
      opts = opts
        |> Keyword.put_new(:username, System.get_env("MDBUSER") || System.get_env("USER"))
        |> Keyword.put_new(:password, System.get_env("MDBPASSWORD"))
        |> Keyword.put_new(:hostname, System.get_env("MDBHOST") || "localhost")
        # Some variable names need to be renamed for mysql driver
        # hostname -> host, username -> user, timeout -> connect_timeout
        |> Keyword.put_new(:host, opts[:hostname])
        |> Keyword.put_new(:user, opts[:username])
        |> Keyword.put_new(:port, opts[:port] || 3306)
        |> Keyword.put_new(:connect_timeout, opts[:timeout] || @timeout)
        |> Keyword.put(:queries, queries)
        |> Keyword.put(:keep_alive, true)
        |> opts_convert_to_char_list


      res = :mysql.start_link(opts)
      case res do
        {:ok, pid} ->
          {:ok, pid}
        {:error, reason} ->
          {:stop, reason}
      end
    end

    @impl true
    def terminate(_, pid) do
      :mysql.stop(pid)
    end

    @impl true
    def handle_call({:query, statement, params, opts}, _, pid) do
      # TODO - add parsing of options, eg. timeout
      cmd = get_command(statement)
      params = params |> Enum.map(fn(el) -> el |> convert_params  end)
      result = case :mysql.query(pid, statement, params) do
        {:ok, columns, rows} ->
          # Convert to correct format for Ecto
          # rows = Enum.map(rows, &List.to_tuple(&1))
          rows = rows |> Enum.map(fn(el) -> Enum.map(el, fn(x) -> if x == :null do nil else x end end) end)
          {:ok, %MyXQLEx.Result{columns: columns, rows: rows, num_rows: length(rows), connection_id: pid} }
        :ok ->
          last_insert_id = :mysql.insert_id(pid)
          affected_rows = :mysql.affected_rows(pid)
          {:ok, %MyXQLEx.Result{columns: [], rows: nil, num_rows: affected_rows, last_insert_id: last_insert_id, connection_id: pid} }
        {:error, {mysql_err_code, _, msg}} ->
          {:error, %MyXQLEx.Error{message: "#{mysql_err_code} - #{msg}"}}
        _ ->
          # Don't crash - but let the user know that this is unhandled.
          {:error, %MyXQLEx.Error{message: "MyXQLEx/connection.ex unhandled match in case statement."}}
      end

      {:reply, result, pid}
    end

    defp convert_params(%NaiveDateTime{} = param), do: NaiveDateTime.to_erl(param)
    defp convert_params(param), do: param

  end