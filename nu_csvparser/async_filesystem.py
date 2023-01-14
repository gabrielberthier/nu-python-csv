from aiofiles import os, open
from os.path import basename


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
