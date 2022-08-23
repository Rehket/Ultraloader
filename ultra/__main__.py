from ultra.sfjwt import jwt_login, load_credentials, CredentialModel
import typer
import sys
import json
from ultra.config import (
    ULTRALOADER_CREDENTIAL_DIRECTORY,
    ULTRALOADER_CREDENTIAL_FILE_PATH,
)
from pathlib import Path

from ultra import bulk2

from ultra.query import query_app
from ultra.ingest import ingest_app

app = typer.Typer()

app.add_typer(query_app, name="query")
app.add_typer(ingest_app, name="ingest")


@app.command()
def login(
    username: str = None,
    client_id: str = None,
    environment: str = None,
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
    Get a bearer token for the provided credentials. The credentials will be saved in
    the user directory under the .ultra

    :param username: The username of the user you wish to authenticate as, it should be in the
        format of an email. Ex: fooBar@gmail.com

    :param client_id: The consumer_id is the client_id of the app you are using to connect with SalesForce.
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
            consumer_id=client_id,
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
            consumer_id=client_id,
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


# noinspection GrazieInspection
@app.command()
def get_job(
    job_id: str,
    version: str = typer.Option(
        "53.0",
        help="The API version to use when creating the job.",
    ),
):
    """
    Get the job details from SalesForce. Prints the output as formatted json.

    :param job_id: The SalesForce Job Id.

    :param version: The API version to use. Defaults to 53.0

    """
    print(json.dumps(obj=bulk2.get_job(job_id=job_id, version=version), indent=2))


if __name__ == "__main__":

    app()
