import numpy as np
from pandas import DataFrame
from asyncio import gather

possible_tags = {
    "Gabs Income": ["Transferência recebida pelo Pix - GABRIEL N BERTHIER DA SILVA"],
    "Entertainment": [
        "tickets",
        "cinema",
    ],
    "Streamming": ["spotify", "netflix", "hbo"],
    "Food": [
        "lanchonete",
        "galeteria",
        "sorvete",
        "hamburguer",
        "pizza",
        "lanche",
        "caffe",
        "caffe",
    ],
    "Rent&Bills": ["caema", "CAEMA", "cemar", "CEMAR", "equatorial"],
    "Travel": ["travel", "latam", "azul", "linhas aéreas"],
    "Transport": ["uber", "99"],
    "Sport": ["gympass", "academia"],
    "Transfer": [
        "transferência",
        "transferencia",
    ],
    "Cash Withdrawal": ["saque"],
    "Gifts": [],
    "Groceries": ["Mateus Supermercado", "supermercado", "mercado"],
    "Dentist": ["Catarina Maura Morais Belo"],
    "Investment": ["Aplicação RDB", "Aplicação Fundo"],
    "Other": [],
}


async def check_matches(df: DataFrame, tag: str, contains: str):
    df["Tags"] = np.where(
        df["Item"].str.contains(contains, case=False), tag, df["Tags"]
    )


async def classify(df: DataFrame):
    awaitables = []
    for k, v in possible_tags.items():
        if len(v):
            v: list = v
            contains = "|".join([a.lower() for a in v])
            awaitables.append(check_matches(df, k, contains))

    return await gather(*awaitables)
