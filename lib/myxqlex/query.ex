defmodule MyXQLEx.Query do
    @type t :: %__MODULE__{
        name: iodata(),
        cache: :reference | :statement,
        num_params: non_neg_integer(),
        statement: iodata()
      }

    defstruct name: "",
          cache: :reference,
          num_params: nil,
          ref: nil,
          statement: nil,
          statement_id: nil
    # defstruct [:statement]
    defimpl DBConnection.Query do
        def parse(query, _opts), do: query
        def describe(query, _opts), do: query
        def encode(_query, params, _opts), do: params
        def decode(_query, result, _opts), do: result
    end
    defimpl String.Chars do
        alias MyXQLEx.Query
        def to_string(%{statement: sttm}) do
        case sttm do
            sttm when is_binary(sttm) -> IO.iodata_to_binary(sttm)
            %{statement: %Query{} = q} -> String.Chars.to_string(q)
        end
        end
    end
end
