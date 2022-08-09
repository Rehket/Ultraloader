from functools import partial
from codecs import iterdecode
import io
from typing import List, Dict, Optional, Union
from pathlib import Path
import csv
from pydantic import BaseModel


class TaskResult(BaseModel):
    status: str
    message: Optional[str]
    payload: Union[List, Dict, str]


def get_target_files(path_or_file: str, pattern: str) -> List[str]:

    path = Path(path_or_file).expanduser()
    if path.exists() and path.is_file():
        return [str(path)]

    elif path.exists() and path.is_dir():
        if pattern is not None:
            return [str(path) for path in path.rglob(pattern)]
        else:
            return [
                str(file_path) for file_path in path.iterdir() if file_path.is_file()
            ]
    else:
        raise RuntimeError(
            f"Invalid file_path or invalid pattern used: path_or_file: {path_or_file}  pattern: {pattern}"
        )


def combine_file_in_buffers(
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
                    output_buffer.seek(0)
                    output_buffer.truncate(0)
    output_buffer.seek(0)
    yield output_buffer


def combine_files(
    path_or_file: str,
    pattern: str,
    output_directory: str,
    file_name: str = "batch",
    file_size_limit: int = 90000000,
) -> TaskResult:
    """
    Combines files matching a glob pattern to be loaded in larger batches.
    """
    file_list = get_target_files(path_or_file, pattern)
    out_path = Path(output_directory).expanduser()
    if not out_path.exists() or out_path.is_file():
        out_path.mkdir(parents=True, exist_ok=True)

    result = TaskResult(status="failed", payload=[])
    try:
        for count, file in enumerate(
            combine_file_in_buffers(files=file_list, file_size_limit=file_size_limit)
        ):
            print(Path(out_path, f"{file_name}_{count}.csv"))
            with open(Path(out_path, f"{file_name}_{count}.csv"), "w") as out:
                out.write(file.read())
                result.payload.append(str(Path(out_path, f"{file_name}_{count}.csv")))
        result.status = "success"
    except Exception as e:
        result.message = str(e)

    return result


if __name__ == "__main__":
    # file_list = get_target_files("./data/rehket-big-load-test", "7505f000002YgowAAC*")
    #
    # for count, file in enumerate(combine_files(files=file_list)):
    #     print(f"./data/out_chunk_{count}.csv")
    #     with open(f"./data/out_chunk_{count}.csv", "w") as out:
    #
    #         out.write(file.read())

    print(
        combine_files(
            "./data/rehket-big-load-test",
            pattern="7505f000002YgowAAC*",
            output_directory="./data/",
        )
    )
