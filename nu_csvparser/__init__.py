import os
from pathlib import Path
import asyncio
import time

from .async_filesystem import read_async_file
from .csv_parser import read_and_transpose_nu_csv


def get_default_files():
    return f"{os.getcwd()}/output", f"{os.getcwd()}/csvs"


async def parse_csv(where: str = None, output_folder: str = None):
    start = time.time()
    default_output_folder, csvs_path = get_default_files()

    where = where or csvs_path
    output_folder = output_folder or default_output_folder

    csv_files = []

    if os.path.isfile(where):
        csv_files.append(where)
    elif os.path.isdir(where):
        csv_files = Path(where).glob("*.csv")
    else:
        raise Exception("The designated source does not exist")

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
