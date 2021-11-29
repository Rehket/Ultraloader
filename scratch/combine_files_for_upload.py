from functools import partial
from codecs import iterdecode
import io
from typing import List
from pathlib import Path
import csv


def get_target_files(path_or_file: str, job_id: str) -> List[str]:
    path = Path(path_or_file)

    files = [
        str(file) for file in path.iterdir() if job_id in str(file) and file.is_file()
    ]

    return files


def combine_files(
    files: List[str], file_size_limit: int = 90000000, output_buffer: io.StringIO = None
) -> io.StringIO:

    if output_buffer is None:
        output_buffer = io.StringIO()

    for file_path in files:
        with open(file_path, newline="") as input_csv:
            header = input_csv.readline()
            for line in input_csv:
                if output_buffer.tell() == 0:
                    output_buffer.write(header)
                output_buffer.write(bytes(line, "utf-8").decode("utf-8", "ignore"))
                if output_buffer.tell() > file_size_limit:
                    output_buffer.seek(0)
                    yield output_buffer
                    buffer.seek(0)
                    buffer.truncate(0)
    output_buffer.seek(0)
    yield output_buffer


if __name__ == "__main__":
    file_list = get_target_files("./data/rehket-big-load-test", "7505f000002YgowAAC")
    # for path in Path("./data/rehket-big-load-test").iterdir():
    #     if "7505f000002YgowAAC" in str(path):
    #         print(path)

    buffer = io.StringIO()
    for count, file in enumerate(combine_files(files=file_list, output_buffer=buffer)):
        print(f"./data/out_chunk_{count}.csv")
        with open(f"./data/out_chunk_{count}.csv", "w") as out:

            out.write(file.read())
