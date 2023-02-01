import typer
import json
from ultra import bulk2

ingest_app = typer.Typer()


@ingest_app.command()
def create_ingest_job(
    object_name: str,
    operation: str,
    external_id_field_name: str = typer.Option(
        None,
        help="The field used to match objects when inserting data.",
    ),
    version: str = typer.Option(
        "53.0",
        help="The API version to use when creating the job.",
    ),
):
    bulk_ingest = bulk2.create_ingest_job(
        object_name=object_name,
        operation=operation,
        external_id_field_name=external_id_field_name,
        version=version,
    )
    print(json.dumps(obj=bulk_ingest, indent=2))


@ingest_app.command()
def load(
    object_name: str,
    operation: str,
    path_or_file: str,
    pattern: str = typer.Option(
        "*",
        help="The field used to match objects when inserting data.",
    ),
    external_id_field_name: str = typer.Option(
        None,
        help="The field used to match objects when inserting data.",
    ),
    version: str = typer.Option(
        "53.0",
        help="The API version to use when creating the job.",
    ),
    batch_size: int = typer.Option(
        90000000,
        help="The API version to use when creating the job.",
    ),
    working_directory: str = typer.Option(
        None,
        help="The directory to use while shifting files.",
    ),
):
    bulk_ingest = bulk2.ingest_job_data_batches(
        object_name=object_name,
        operation=operation,
        path_or_file=path_or_file,
        pattern=pattern,
        batch_size=batch_size,
        working_directory=working_directory,
        external_id_field_name=external_id_field_name,
        version=version,
    )
    print(json.dumps(obj=bulk_ingest, indent=2))


@ingest_app.command()
def load_ingest_job_data(
    job_id: str,
    file_path: str,
    version: str = typer.Option(
        "53.0",
        help="The API version to use when creating the job.",
    ),
):
    print(
        bulk2.load_ingest_job_data(
            job_id=job_id,
            file_path=file_path,
            version=version,
        )
    )
