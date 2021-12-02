from sfjwt import jwt_login, load_credentials, CredentialModel
import typer
import sys
import json
from config import ULTRALOADER_CREDENTIAL_DIRECTORY, ULTRALOADER_CREDENTIAL_FILE_PATH
from pathlib import Path

import bulk2

app = typer.Typer()


@app.command()
def login(
    username: str,
    consumer_id: str,
    environment: str,
    private_key: str = typer.Option(
        None,
        help="The path to the private key file. If None, the environment variable will be checked instead.",
    ),
    json_credential_file: str = typer.Option(
        None,
        help="The path to a json credential file containing the client_id, secret_key, username, and if a sandbox is used.",
    ),
    json_out: bool = typer.Option(
        False,
        help="Print the bearer token and url to the terminal after authentication.",
    ),
):
    """
    Get a bearer token for the provided provided credentials. The credentials will be saved in
    the user directory under the .sofa

    :param username: The username of the user you wish to authenticate as, it should be in the
        format of an email. Ex: fooBar@gmail.com

    :param consumer_id: The consumer_id is the client_id of the app you are using to connect with SalesForce.
        The consumer key is like the username for the connected app.

    :param private_key: The path to the private key. The private_key is the private certificate used to sign the
        requests for an auth token. It is imperative to keep the key secure. When using a private key in an enabled app,
        more than just one user can be comprised in the event the cert is stolen.

    :param environment: The environment determines whether to use SalesForce's test or production authentication
        server. If sandbox is passed, the test server is used, if production is used, the auth request is sent to the
        production server. Other values raise an exception.

    :param json_credential_file: Path to a json credential file containing the client_id, secret_key, username,
        and if a sandbox is used.

    :param json_out: If True, the url and bearer token are printed to stdout and will not be
        saved to the .sofa directory.

    """
    try:
        credentials = load_credentials(
            username=username,
            consumer_id=consumer_id,
            private_key=private_key,
            environment=environment,
            json_credential_file=json_credential_file,
        )
    except Exception as e:
        print(f"Error while loading credentials: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        instance_url, token = jwt_login(
            **credentials.dict(
                exclude_unset=True, exclude_none=True, exclude={"instance_url"}
            )
        )
        credential = CredentialModel(
            username=username,
            consumer_id=consumer_id,
            environment=environment,
            instance_url=instance_url,
            token=token,
        )
    except Exception as e:
        print(
            f"Error while authenticating, double-check your credentials: {e}",
            file=sys.stderr,
        )
        sys.exit(1)

    if json_out:
        print(
            credential.json(
                exclude_none=True, exclude_unset=True, exclude={"private_key"}, indent=2
            ),
            file=sys.stdout,
        )
    else:
        credential_directory = Path(ULTRALOADER_CREDENTIAL_DIRECTORY).expanduser()
        if not credential_directory.exists():
            credential_directory.mkdir(parents=True)
        with open(Path(ULTRALOADER_CREDENTIAL_FILE_PATH).expanduser(), "w") as creds:
            creds.write(
                credential.json(
                    exclude_none=True,
                    exclude_unset=True,
                    exclude={"private_key"},
                    indent=2,
                )
            )

        print(f"Credentials Saved! You're all set.", file=sys.stdout)


@app.command()
def create_query_job(
    query: str,
    version: str = typer.Option(
        "53.0",
        help="The API version to use when creating the job.",
    ),
):

    print(
        json.dumps(obj=bulk2.create_query_job(query=query, version=version), indent=2)
    )


@app.command()
def get_query_job(
    job_id: str,
    version: str = typer.Option(
        "53.0",
        help="The API version to use when creating the job.",
    ),
):

    print(json.dumps(obj=bulk2.get_query_job(job_id=job_id, version=version), indent=2))


@app.command()
def get_job(
    job_id: str,
    version: str = typer.Option(
        "53.0",
        help="The API version to use when creating the job.",
    ),
):

    print(json.dumps(obj=bulk2.get_job(job_id=job_id, version=version), indent=2))


@app.command()
def download_query_data(
    job_id: str,
    version: str = typer.Option(
        "53.0",
        help="The API version to use when creating the job.",
    ),
):
    bulk2.download_query_data(job_id=job_id, version=version)


@app.command()
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


@app.command()
def ingest_job_data_batches(
    object_name: str,
    operation: str,
    path_or_file: str,
    pattern: str,
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


@app.command()
def load_ingest_job_data(
    job_id: str,
    file_path: str,
    # pattern: str,
    # batch_size: int = typer.Option(
    #     90000000,
    #     help="The API version to use when creating the job.",
    # ),
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


if __name__ == "__main__":

    app()
