from pandas import DataFrame

map_values_debts = {
    "Compra no débito": "💸 Débito",
    "Transferência enviada pelo Pix": "🪙 Pix",
    "Aplicação RDB": "💰 Investimento RDB",
    "Aplicação Fundo": "🏦 Aplicação Fundo",
    "Pagamento de Fatura": "💳 Fatura",
}

map_values_incomes = {
    "Transferência Recebida -": "🤑 Transferência",
    "Transferência recebida pelo Pix": "🪙 Pix",
}


def map_entry_item(df: DataFrame, frame_type: str):
    map_values = None

    if frame_type == "expenses":
        map_values = map_values_debts
    else:
        map_values = map_values_incomes

    def set_item_name(item):
        matchers = [a for a in map_values.keys()]
        key = [x for x in matchers if x in item]

        return map_values[key[0]] if len(key) > 0 else item

    df["Item"] = df["Item"].map(
        lambda x: set_item_name(x),
    )

    return df
