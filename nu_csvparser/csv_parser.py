import pandas as pd
import asyncio
from aiofiles import os, open
from pathlib import Path
from os.path import basename
from io import StringIO
from csv_pipeline import transform
import time


def get_default_files():
    import os

    return f"{os.getcwd()}/output", f"{os.getcwd()}/csvs"


async def create_dirs_if_not_exist(output_folder: str):
    outputs = ["", "expenses", "incomes"]
    for o in outputs:
        trimmed = f"{output_folder}/{o}".strip()
        if not (await os.path.exists(trimmed)):
            await os.makedirs(trimmed)
            print("Created" + f"{trimmed}")


async def read_async_file(file_path):
    print(f"Parsing file {file_path}")
    async with open(file_path, mode="r") as f:
        return (basename(file_path), await f.read())


async def save_csv(
    df: pd.DataFrame, output_folder: str, filename: str, csv_type: str
) -> None:
    output = f"{output_folder}/{csv_type}/{filename}"
    print(f"Destination: {output}")

    print("Sum of " + output)

    print(f'{csv_type}: {df["Amount"].sum()}')

    df.to_csv(output, index=False)


async def separate_expenses_and_incomes(df: pd.DataFrame):
    expenses = df.loc[df["Amount"] < 0].copy()
    incomes = df.loc[df["Amount"] > 0].copy()

    # Treat expense values as absolute values so operations should work easier
    expenses["Amount"] = expenses["Amount"].map(
        lambda x: abs(x),
    )

    return expenses, incomes


async def read_and_transpose_nu_csv(csv: str, output: str, filename: str):
    with StringIO(csv) as text_io:
        nu_data: pd.DataFrame = pd.read_csv(text_io)

        set_rows = {"Data": "Date", "Valor": "Amount", "Descrição": "Item"}
        nu_data.rename(columns=set_rows, inplace=True)
        new_dataframe = nu_data.assign(Tags=lambda x: "")

        expenses, incomes = await separate_expenses_and_incomes(new_dataframe)

        df1, df2 = await asyncio.gather(
            transform(expenses, "expenses"), transform(incomes, "incomes")
        )

        return await asyncio.gather(
            save_csv(
                df1,
                output,
                filename,
                "expenses",
            ),
            save_csv(df2, output, filename, "incomes"),
        )


async def main():
    start = time.time()
    # gets default output folder and csvs folder
    output_folder, csvs_path = get_default_files()
    # creates all necessary directories
    await create_dirs_if_not_exist(output_folder)

    # path from which csvs are loaded
    path = csvs_path

    # reads all csv files (by .csv extension)
    csv_files = Path(path).glob("*.csv")

    # Transform reading in async process for every input
    futures = [read_async_file(f) for f in csv_files]

    # Awaits for futures to settle so then we can use them
    files = await asyncio.gather(*futures)

    # Transforms reading and saving functions in task array
    tasks = [
        asyncio.create_task(
            read_and_transpose_nu_csv(
                csv=content, output=output_folder, filename=filename
            )
        )
        for filename, content in files
    ]

    # Run all until complete
    await asyncio.wait(tasks)

    print("Parsing complete")
    end = time.time()

    time_spent = end - start

    print(f"Total time spent: {time_spent}s")


if __name__ == "__main__":
    asyncio.run(main())
