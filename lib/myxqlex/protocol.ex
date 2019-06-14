defmodule MyXQLEx.Protocol do

    use DBConnection
    alias MyXQLEx.Proxy
    alias MyXQLEx.Query, as: Q
    alias DBConnection.ConnectionError
    require Logger

    @impl true
    def connect(opts) do
      {:ok, pid} = Proxy.start_link(opts)
      {:ok, %{proxy_pid: pid}}
    end

    @impl true
    def disconnect(_err, state) do
      case Proxy.stop(state.proxy_pid) do
        :ok -> :ok
        {:error, reason} -> {:error, reason, state}
      end
    end

    # @impl true
    # def handle_prepare(query, opts, state) do
    #   query = if state.prepare == :unnamed, do: %{query | name: ""}, else: query

    # #   if cached_query = queries_get(state, query) do
    # #     {:ok, cached_query, state}
    # #   else
    #     case prepare(query, state) do
    #       {:ok, query, state} ->
    #         {:ok, query, state}

    #       {:error, %MyXQLEx.Error{mysqlex: %{name: :ER_UNSUPPORTED_PS}}, state} = error ->
    #         if Keyword.get(opts, :query_type) == :binary_then_text do
    #           query = %MyXQLEx.TextQuery{statement: query.statement}
    #           {:ok, query, state}
    #         else
    #           error
    #         end

    #       other ->
    #         other
    #     end
    # #   end
    # end

    @impl true
    def handle_execute(%Q{} = query, params, _opts, state) do
      response = Proxy.query(state.proxy_pid, query.statement, params)
      case response do
        {:ok, _, _} = res ->
          {:ok, query, res, state}
        {:ok, resp} = res ->
            {:ok, query, resp, state}
        {:error, _} ->
          message = "Query execution error"
          exception = %ConnectionError{message: message}
          {:disconnect, exception, state}
        other ->
            {:ok, query, other, state}
      end
    end

    @impl true
    def checkin(state), do: {:ok, state}

    @impl true
    def checkout(state), do: {:ok, state}

    @impl true
    def ping(state), do: {:ok, state}

    @impl true
    def handle_prepare(query, _opts, state), do: {:ok, query, state}

    @impl true
    def handle_close(query, _opts, state), do: {:ok, query, state}

    # Not implemented

    @impl true
    def handle_declare(_query, _params, _opts, _state) do
      not_implemented()
    end

    @impl true
    def handle_deallocate(_query, _cursor, _opts, _state) do
      not_implemented()
    end

    @impl true
    def handle_begin(_opts, _state) do
      not_implemented()
    end

    @impl true
    def handle_commit(_opts, _state) do
      not_implemented()
    end

    @impl true
    def handle_rollback(_opts, _state) do
      not_implemented()
    end

    @impl true
    def handle_fetch(_query, _cursor, _opts, _state) do
      not_implemented()
    end

    @impl true
    def handle_status(_opts, _state) do
      not_implemented()
    end

    defp not_implemented, do: raise("Not implemented")
  end