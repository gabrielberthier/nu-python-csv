from pandas import DataFrame
from datetime import datetime
import calendar


def insert_columns(df: DataFrame):
    df["Month"] = df.apply(
        lambda x: calendar.month_name[datetime.strptime(x["Date"], "%d/%m/%Y").month],
        axis=1,
    )

    df["Description"] = df.apply(
        lambda x: x["Item"],
        axis=1,
    )

    return df
