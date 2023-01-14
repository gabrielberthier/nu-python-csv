from .apply_cols import insert_columns
from .map_extract_entries import map_entry_item
from pandas import DataFrame

from .parallel_mapper import parallelize_dataframe


def pipeline_income(df: DataFrame):
    df = insert_columns(df)
    df = map_entry_item(df, "incomes")

    return df


def pipeline_expense(df: DataFrame):
    df = insert_columns(df)
    df = map_entry_item(df, "expenses")

    return df


async def transform(df: DataFrame, pipe_type: str):
    pipeline = pipeline_expense if pipe_type == "expenses" else pipeline_income
    return parallelize_dataframe(df, pipeline)
