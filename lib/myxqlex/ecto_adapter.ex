
defmodule MyXQLEx.EctoAdapter do
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
    def loaders(:binary_id, type),      do: [Ecto.UUID, type]
    def loaders(_, type),               do: [type]

    defp bool_decode(<<0>>), do: {:ok, false}
    defp bool_decode(<<1>>), do: {:ok, true}
    defp bool_decode(0), do: {:ok, false}
    defp bool_decode(1), do: {:ok, true}
    defp bool_decode(x), do: {:ok, x}

    defp float_decode(%Decimal{} = decimal), do: {:ok, Decimal.to_float(decimal)}
    defp float_decode(x), do: {:ok, x}

    defp json_decode(x) when is_binary(x), do: {:ok, MyXQLEx.json_library().decode!(x)}
    defp json_decode(x), do: {:ok, x}

end