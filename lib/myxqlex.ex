defmodule MyXQLEx do
  @moduledoc """
  Documentation for Myxqlex.
  """

  @doc """
  Hello world.

  ## Examples

      iex> Myxqlex.hello()
      :world

  """
  alias MyXQLEx.Query

  @type conn() :: DBConnection.conn()

  @type start_option() ::
          {:protocol, :socket | :tcp}
          | {:socket, Path.t()}
          | {:socket_options, [:gen_tcp.connect_option()]}
          | {:hostname, String.t()}
          | {:port, :inet.port_number()}
          | {:database, String.t() | nil}
          | {:username, String.t()}
          | {:password, String.t() | nil}
          | {:ssl, boolean()}
          | {:ssl_opts, [:ssl.tls_client_option()]}
          | {:connect_timeout, timeout()}
          | {:handshake_timeout, timeout()}
          | {:ping_timeout, timeout()}
          | {:prepare, :named | :unnamed}
          | {:disconnect_on_error_codes, [atom()]}
          | DBConnection.start_option()

  @type option() :: DBConnection.option()

  # def child_spec(opts) do
  #   DBConnection.child_spec(MyXQLEx.Protocol, opts)
  # end

  # def execute(conn, query, params, opts) do
  #   query = %Query{statement: query}
  #   DBConnection.prepare_execute(conn, query, params, opts)
  # end

  # def execute(conn, _, query, params, opts) do
  #   query = %Query{statement: query}
  #   DBConnection.prepare_execute(conn, query, params, opts)
  # end

  defmacrop is_iodata(data) do
    quote do
      is_list(unquote(data)) or is_binary(unquote(data))
    end
  end

  @doc """
  Runs a query.

  ## Text queries and prepared statements

  MyXQLEx supports MySQL's two ways of executing queries:

    * text protocol - queries are sent as text

    * binary protocol - used by prepared statements

      The query statement is still sent as text, however it may contain placeholders for parameter
      values.

      Prepared statements have following benefits:

        * better performance: less overhead when parsing the query by the DB engine
        * better performance: binary protocol for encoding parameters and decoding result sets is more efficient
        * protection against SQL injection attacks

      The drawbacks of prepared statements are:

        * not all statements are preparable
        * requires two roundtrips to the DB server: one for preparing the statement and one for executing it.
          This can be alleviated by holding on to prepared statement and executing it multiple times.

  ## Options

    * `:query_type` - use `:binary` for binary protocol (prepared statements), `:binary_then_text` to attempt
      executing a binary query and if that fails fallback to executing a text query, and `:text` for text protocol
      (default: `:binary`)

  Options are passed to `DBConnection.execute/4` for text protocol, and
  `DBConnection.prepare_execute/4` for binary protocol. See their documentation for all available
  options.

  ## Examples

      iex> MyXQLEx.query(conn, "CREATE TABLE posts (id serial, title text)")
      {:ok, %MyXQLEx.Result{}}

      iex> MyXQLEx.query(conn, "INSERT INTO posts (title) VALUES ('title 1')")
      {:ok, %MyXQLEx.Result{last_insert_id: 1, num_rows: 1}}

      iex> MyXQLEx.query(conn, "INSERT INTO posts (title) VALUES (?)", ["title 2"])
      {:ok, %MyXQLEx.Result{last_insert_id: 2, num_rows: 1}}

  """
  @spec query(conn, iodata, list, [option()]) ::
          {:ok, MyXQLEx.Result.t()} | {:error, Exception.t()}
  def query(conn, statement, params \\ [], options \\ []) when is_iodata(statement) do
    if name = Keyword.get(options, :cache_statement) do
      statement = IO.iodata_to_binary(statement)
      query = %MyXQLEx.Query{name: name, statement: statement, cache: :statement, ref: make_ref()}

      DBConnection.prepare_execute(conn, query, params, options)
      |> query_result()
    else
      do_query(conn, statement, params, options)
    end
  end

  defp do_query(conn, statement, params, options) do
    case Keyword.get(options, :query_type, :binary) do
      :binary ->
        prepare_execute(conn, "", statement, params, options)

      :binary_then_text ->
        prepare_execute(conn, "", statement, params, options)

      :text ->
        DBConnection.execute(conn, %MyXQLEx.TextQuery{statement: statement}, params, options)
    end
    |> query_result()
  end

  defp query_result({:ok, _query, result}), do: {:ok, result}
  defp query_result({:error, _} = error), do: error

  @doc """
  Runs a query.

  Returns `%MyXQLEx.Result{}` on success, or raises an exception if there was an error.

  See `query/4`.
  """
  @spec query!(conn, iodata, list, [option()]) :: MyXQLEx.Result.t()
  def query!(conn, statement, params \\ [], opts \\ []) do
    case query(conn, statement, params, opts) do
      {:ok, result} -> result
      {:error, exception} -> raise exception
    end
  end

  @doc """
  Prepares a query to be later executed.

  To execute the query, call `execute/4`. To close the query, call `close/3`.

  ## Options

  Options are passed to `DBConnection.prepare/4`, see it's documentation for
  all available options.

  ## Examples

      iex> {:ok, query} = MyXQLEx.prepare(conn, "", "SELECT ? * ?")
      iex> {:ok, %MyXQLEx.Result{rows: [row]}} = MyXQLEx.execute(conn, query, [2, 3])
      iex> row
      [6]

  """
  @spec prepare(conn(), iodata(), iodata(), [option()]) ::
          {:ok, MyXQLEx.Query.t()} | {:error, Exception.t()}
  def prepare(conn, name, statement, opts \\ []) when is_iodata(name) and is_iodata(statement) do
    query = %MyXQLEx.Query{name: name, statement: statement, ref: make_ref()}
    DBConnection.prepare(conn, query, opts)
  end

  @doc """
  Prepares a query.

  Returns `%MyXQLEx.Query{}` on success, or raises an exception if there was an error.

  See `prepare/4`.
  """
  @spec prepare!(conn(), iodata(), iodata(), [option()]) :: MyXQLEx.Query.t()
  def prepare!(conn, name, statement, opts \\ []) when is_iodata(name) and is_iodata(statement) do
    query = %MyXQLEx.Query{name: name, statement: statement, ref: make_ref()}
    DBConnection.prepare!(conn, query, opts)
  end

  @doc """
  Prepares and executes a query in a single step.

  ## Multiple results

  If a query returns multiple results (e.g. it's calling a procedure that returns multiple results)
  an error is raised. If a query may return multiple results it's recommended to use `stream/4` instead.

  ## Options

  Options are passed to `DBConnection.prepare_execute/4`, see it's documentation for
  all available options.

  ## Examples

      iex> {:ok, _query, %MyXQLEx.Result{rows: [row]}} = MyXQLEx.prepare_execute(conn, "", "SELECT ? * ?", [2, 3])
      iex> row
      [6]

  """
  @spec prepare_execute(conn, iodata, iodata, list, keyword()) ::
          {:ok, MyXQLEx.Query.t(), MyXQLEx.Result.t()} | {:error, Exception.t()}
  def prepare_execute(conn, name, statement, params \\ [], opts \\ [])
      when is_iodata(name) and is_iodata(statement) do
    query = %MyXQLEx.Query{name: name, statement: statement, ref: make_ref()}
    DBConnection.prepare_execute(conn, query, params, opts)
  end

  @doc """
  Prepares and executes a query in a single step.

  Returns `{%MyXQLEx.Query{}, %MyXQLEx.Result{}}` on success, or raises an exception if there was
  an error.

  See: `prepare_execute/5`.
  """
  @spec prepare_execute!(conn, iodata, iodata, list, [option()]) ::
          {MyXQLEx.Query.t(), MyXQLEx.Result.t()}
  def prepare_execute!(conn, name, statement, params \\ [], opts \\ [])
      when is_iodata(name) and is_iodata(statement) do
    query = %MyXQLEx.Query{name: name, statement: statement, ref: make_ref()}
    DBConnection.prepare_execute!(conn, query, params, opts)
  end

  @doc """
  Executes a prepared query.

  ## Options

  Options are passed to `DBConnection.execute/4`, see it's documentation for
  all available options.

  ## Examples

      iex> {:ok, query} = MyXQLEx.prepare(conn, "", "SELECT ? * ?")
      iex> {:ok, %MyXQLEx.Result{rows: [row]}} = MyXQLEx.execute(conn, query, [2, 3])
      iex> row
      [6]

  """
  @spec execute(conn(), MyXQLEx.Query.t(), list(), [option()]) ::
          {:ok, MyXQLEx.Query.t(), MyXQLEx.Result.t()} | {:error, Exception.t()}
  defdelegate execute(conn, query, params, opts \\ []), to: DBConnection

  @doc """
  Executes a prepared query.

  Returns `%MyXQLEx.Result{}` on success, or raises an exception if there was an error.

  See: `execute/4`.
  """
  @spec execute!(conn(), MyXQLEx.Query.t(), list(), keyword()) :: MyXQLEx.Result.t()
  defdelegate execute!(conn, query, params, opts \\ []), to: DBConnection

  @doc """
  Closes a prepared query.

  Returns `:ok` on success, or raises an exception if there was an error.

  ## Options

  Options are passed to `DBConnection.close/3`, see it's documentation for
  all available options.
  """
  @spec close(conn(), MyXQLEx.Query.t(), [option()]) :: :ok
  def close(conn, %MyXQLEx.Query{} = query, opts \\ []) do
    case DBConnection.close(conn, query, opts) do
      {:ok, _} ->
        :ok

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Acquire a lock on a connection and run a series of requests inside a
  transaction. The result of the transaction fun is return inside an `:ok`
  tuple: `{:ok, result}`.

  To use the locked connection call the request with the connection
  reference passed as the single argument to the `fun`. If the
  connection disconnects all future calls using that connection
  reference will fail.

  `rollback/2` rolls back the transaction and causes the function to
  return `{:error, reason}`.

  `transaction/3` can be nested multiple times if the connection
  reference is used to start a nested transaction. The top level
  transaction function is the actual transaction.

  ## Options

  Options are passed to `DBConnection.transaction/3`, see it's documentation for
  all available options.

  ## Examples

      {:ok, result} =
        MyXQLEx.transaction(pid, fn conn  ->
          MyXQLEx.query!(conn, "SELECT title FROM posts")
        end)

  """
  @spec transaction(conn, (DBConnection.t() -> result), [option()]) ::
          {:ok, result} | {:error, any}
        when result: var
  defdelegate transaction(conn, fun, opts \\ []), to: DBConnection

  @doc """
  Rollback a transaction, does not return.

  Aborts the current transaction. If inside multiple `transaction/3`
  functions, bubbles up to the top level.

  ## Example

      {:error, :oops} =
        MyXQLEx.transaction(pid, fn conn  ->
          MyXQLEx.rollback(conn, :oops)
          IO.puts "never reaches here!"
        end)

  """
  @spec rollback(DBConnection.t(), any()) :: no_return()
  defdelegate rollback(conn, reason), to: DBConnection

  @doc """
  Returns a stream for a query on a connection.

  Stream consumes memory in chunks of at most `max_rows` rows (see Options).
  This is useful for processing _large_ datasets.

  A stream must be wrapped in a transaction and may be used as an `Enumerable`.

  ## Options

    * `:max_rows` - Maximum numbers of rows in a result (default: `500`)

  Options are passed to `DBConnection.stream/4`, see it's documentation for
  other available options.

  ## Examples

      {:ok, results} =
        MyXQLEx.transaction(pid, fn conn ->
          stream = MyXQLEx.stream(conn, "SELECT * FROM integers", [], max_rows: max_rows)
          Enum.to_list(stream)
        end)

  Suppose the `integers` table contains rows: 1, 2, 3, 4 and `max_rows` is set to `2`.
  We'll get following results:

      # The first item is result of executing the query and has no rows data
      Enum.at(results, 0)
      #=> %MyXQLEx.Result{num_rows: 0, ...}

      # The second item is result of fetching rows 1 & 2
      Enum.at(results, 1)
      #=> %MyXQLEx.Result{num_rows: 2, rows: [[1], [2]]}

      # The third item is result of fetching rows 3 & 4
      Enum.at(results, 2)
      #=> %MyXQLEx.Result{num_rows: 2, rows: [[3], [4]]}

  Because the total number of fetched rows happens to be divisible by our chosen `max_rows`,
  there might be more data on the server so another fetch attempt is made.
  Because in this case there weren't any more rows, the final result has 0 rows:

      Enum.at(results, 3)
      #=> %MyXQLEx.Result{num_rows: 0}

  However, if the table contained only 3 rows, the 3rd result would contain:

      Enum.at(results, 2)
      #=> %MyXQLEx.Result{num_rows: 1, rows: [[3]]}

  And that would be the last result in the stream.
  """
  @spec stream(DBConnection.t(), iodata | MyXQLEx.Query.t(), list, [option()]) ::
          DBConnection.PrepareStream.t()
  def stream(conn, query, params \\ [], opts \\ [])

  def stream(%DBConnection{} = conn, statement, params, opts) when is_iodata(statement) do
    query = %MyXQLEx.Query{
      name: "",
      ref: make_ref(),
      statement: statement,
      num_params: length(params)
    }

    stream(conn, query, params, opts)
  end

  def stream(%DBConnection{} = conn, %MyXQLEx.Query{} = query, params, opts) do
    opts = Keyword.put_new(opts, :max_rows, 500)
    DBConnection.prepare_stream(conn, query, params, opts)
  end

  @doc """
  Returns a supervisor child specification for a DBConnection pool.
  """
  @spec child_spec([start_option()]) :: Supervisor.child_spec()
  def child_spec(opts) do
    ensure_deps_started!(opts)
    DBConnection.child_spec(MyXQLEx.Protocol, opts)
  end

  @doc """
  Returns the configured JSON library.

  To customize the JSON library, include the following in your `config/config.exs`:

      config :myxqlex, :json_library, SomeJSONModule

  Defaults to `Jason`.
  """
  @spec json_library() :: module()
  def json_library() do
    Application.get_env(:myxqlex, :json_library, Jason)
  end

  ## Helpers

  defp ensure_deps_started!(opts) do
    if Keyword.get(opts, :ssl, false) and
         not List.keymember?(:application.which_applications(), :ssl, 0) do
      raise """
      SSL connection cannot be established because `:ssl` application is not started,
      you can add it to `:extra_applications` in your `mix.exs`:

          def application() do
            [extra_applications: [:ssl]]
          end

      """
    end
  end


end
