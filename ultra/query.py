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
        )
):
    print(
        json.dumps(obj=bulk2.create_query_job(query=query, version=version), indent=2),
        file=stdout
    )


@query_app.command()
def download_data(
        job_id: str,
        path: str = typer.Option(
            ".",
            help="The path to download the files to, defaults to current working directory.d",
        ),
        version: str = typer.Option(
            "53.0",
            help="The API version to use when creating the job.",
        ),
):
    bulk2.download_query_data(job_id=job_id, version=version)


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
        )
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
    bulk2.download_query_data(job_id=job_id, version=version)
