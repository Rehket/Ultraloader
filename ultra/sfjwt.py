from httpx import Client  # Used to make http/s requests
import jwt  # JWT Library
import datetime
from typing import Tuple
import json
from pydantic import BaseModel
import sys
import pathlib
from typing import Optional
from os import environ
from ultra import config


class CredentialModel(BaseModel):
    private_key: Optional[str]
    username: str
    consumer_id: str
    environment: str
    instance_url: Optional[str]
    token: Optional[str]
    client_timeout: int = 10
    download_timeout: int = 60
    client_connect_timeout: int = 60


def _load_private_key(path_or_buffer: Optional[str] = None) -> str:
    if path_or_buffer is not None:
        if not pathlib.Path(path_or_buffer).exists():
            raise FileNotFoundError(
                "The private key could not be found at the provided path."
            )
        with open(path_or_buffer) as key_in:
            return key_in.read()
    else:
        private_key = environ.get("SFDC_PRIVATE_KEY", None)
        if private_key is None:
            raise EnvironmentError(
                "No private key is configured. For more help with this error, reference the login command."
            )
        return private_key


def jwt_login(
    consumer_id: str,
    username: str,
    private_key: str,
    environment: str,
    client: Client = None,
) -> Tuple[str, str]:
    client = Client() if client is None else client

    if environment:
        if environment.lower() == "sandbox":
            client.base_url = "https://test.salesforce.com"
        elif environment.lower() == "production":
            client.base_url = "https://login.salesforce.com"
        else:
            raise EnvironmentError(
                f"SFDC_SANDBOX_ENVIRONMENT must be sandbox or production, got {environment}"
            )
    else:
        raise EnvironmentError(
            f"SFDC_SANDBOX_ENVIRONMENT must be sandbox or production"
        )

    jwt_payload = jwt.encode(
        {
            "exp": datetime.datetime.utcnow() + datetime.timedelta(seconds=30),
            "iss": consumer_id,
            "aud": str(client.base_url),
            "sub": username,
        },
        private_key,
        algorithm="RS256",
    )

    # This makes a request againts the oath service endpoint in SFDC.
    # There are two urls, login.salesforce.com for Production and test.salesforce.com
    # for sanboxes/dev/testing environments. When using test.salesforce.com,
    # the sandbox name should be appended to the username.

    result = client.post(
        # https://login.salesforce.com/services/oauth2/token -> PROD
        # https://test.salesforce.com/services/oauth2/token -> sandbox
        url="/services/oauth2/token",
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": jwt_payload,
        },
    )
    body = result.json()
    if result.status_code != 201 and result.status_code != 200:
        print(result.status_code)
        raise RuntimeError(
            f"Authentication Failed: <error: {body['error']}, description: {body['error_description']}>"
        )
    return str(body["instance_url"]), str(body["access_token"])


# TODO: Really Need Tests For This
def load_credentials(
    username: str = None,
    consumer_id: str = None,
    private_key: str = None,
    environment: str = None,
    json_credential_file: str = config.ULTRALOADER_CREDENTIAL_FILE_PATH,
) -> CredentialModel:

    """
    Loads the credentials to a dictionary and raises exceptions if the credentials are specified incorrectly.
    If the credential file is specified, the other values must be None.

    :param username: The Username of the user to connect as.

    :param consumer_id: The consumer_id of the connected app to connect with.

    :param private_key: The path to private key used to sign the auth request. If None, the Environment
        variable is checked instead. If it is not found at any of those locations, an exception is raised.

    :param environment: Whether the environment is a sandbox or production.

    :param json_credential_file: A file path to a file containing the credentials as a json object.

    :return: The Credential Model containing the loaded data.
    """

    if (
        not (username and consumer_id and environment)
        and json_credential_file is not None
        and pathlib.Path(json_credential_file).expanduser().is_file()
    ):
        try:
            with open(
                pathlib.Path(json_credential_file).expanduser(), "r"
            ) as json_cred_file:
                return CredentialModel(**json.load(json_cred_file))
        except FileNotFoundError as e:
            print(
                f"Private key not found at: {private_key}, make sure it's there!",
                file=sys.stderr,
            )
            exit(sys.exit(1))

    if json_credential_file is None and (username and consumer_id and environment):
        private_key = _load_private_key(private_key)
        return CredentialModel(
            username=username,
            consumer_id=consumer_id,
            private_key=private_key,
            environment=environment.lower(),
        )

    else:
        raise RuntimeError(
            f"Only a credential file or a set of raw credentials may be passed to the load_credentials method. "
        )
