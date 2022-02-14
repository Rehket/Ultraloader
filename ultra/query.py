import sys

import typer
import json
from ultra import bulk2
from sys import stdout
from time import sleep

query_app = typer.Typer()


@query_app.command()
def create_job(
    query: str,
    version: str = typer.Option(
        "53.0",
        help="The API version to use when creating the job.",
    ),
):
    print(
        json.dumps(obj=bulk2.create_query_job(query=query, version=version), indent=2),
        file=stdout,
    )


@query_app.command()
def download_data(
    job_id: str = typer.Argument(default=None, help="The job id to download."),
    download_path: str = typer.Option(
        "./data",
        help="The path to download the files to, defaults './data' in the current working directory.",
    ),
    version: str = typer.Option(
        "53.0",
        help="The API version to use when creating the job.",
    ),
    batch_size: int = typer.Option(
        10000,
        help="The number of records to pull in a batch.",
    ),
):

    print(
        bulk2.download_query_data(
            job_id=job_id,
            version=version,
            download_path=download_path,
            batch_size=batch_size,
        ),
        file=sys.stdout,
    )


@query_app.command()
def run(
    query: str,
    version: str = typer.Option(
        "53.0",
        help="The API version to use when creating the job.",
    ),
    check_interval: int = typer.Option(
        5,
        help="How long to wait between job status checks",
    ),
    download_path: str = typer.Option(
        "./data",
        help="The path to download the files to, defaults './data' in the current working directory.",
    ),
):
    """
    Creates a query job and polls until the job is complete before downloading the data.
    """

    query_job = bulk2.create_query_job(query=query, version=version)
    job_id = query_job.get("id")
    job_status = query_job.get("state")
    while job_status in ["UploadComplete", "InProgress"]:
        query_job = bulk2.get_job(job_id=job_id, version=version)
        job_status = query_job.get("state")
        sleep(check_interval)

    print(
        bulk2.download_query_data(
            job_id=job_id, version=version, download_path=download_path
        ),
        file=sys.stdout,
    )
