import httpx  # Used to make http/s requests
import jwt  # JWT Library
import datetime
from typing import Tuple
import json
from pydantic import BaseModel, BaseSettings, Field, validator
import sys
import pathlib
from typing import Optional
from os import environ
from ultra import config
from ultra.exceptions import SalesForceAuthException, SalesForceUserException
from cryptography.fernet import Fernet
import os
import ctypes


class SalesforceCred(BaseSettings):
    username: Optional[str]
    client_id: Optional[str]
    client_secret: Optional[str]
    private_key: Optional[str]
    private_key_filepath: Optional[str]
    environment: Optional[str]
    token: Optional[str]
    base_url: Optional[str]
    api_version: str
    client_timeout: int = 10
    client_connect_timeout: int = 60
    credential_file_path: str = str(pathlib.Path(pathlib.Path().home(), ".ultra/"))

    class Config:
        env_prefix = "sfdc_"
        case_sensitive = False

    @validator("private_key_filepath")
    def validate_private_key_file(cls, v, values):
        if v is None or v == "":
            return v
        if not pathlib.Path(v).exists():
            raise FileNotFoundError(
                f"The private key could not be found at the provided path: {values['private_key_filepath']}"
            )
        with open(pathlib.Path(v)) as private_key_in:
            values["private_key"] = private_key_in.read()
            return v


def _get_or_create_secret():
    """
    Reads or generates the secret used to save/load credentials
    :return:
    """
    FILE_ATTRIBUTE_HIDDEN = 0x02
    ultra_path = pathlib.Path(pathlib.Path.home(), ".ultra")
    ultra_path.mkdir(exist_ok=True)
    key_path = pathlib.Path(ultra_path, ".ultra.key")

    if key_path.exists():
        with open(key_path, "rb") as key_file:
            return Fernet(key_file.read())
    else:
        key_data = Fernet.generate_key()
        with open(key_path, "wb") as key_file:
            key_file.write(key_data)

        if os.name == "nt":
            ret = ctypes.windll.kernel32.SetFileAttributesW(
                key_path, FILE_ATTRIBUTE_HIDDEN
            )
            if not ret:
                # There was an error.
                raise ctypes.WinError()
        return Fernet(key_data)


def login(alias: str):
    """
    Login using the Provided Alias.
    :param alias:
    :return:
    """
    pass


def connect():
    """
    Connect to a new Org.
    :return:
    """
    pass


def list_orgs():
    """
    List the connected Orgs and the time the token expires.
    :return:
    """
    pass


def remove(alies: str):
    """
    Remove the org by alias
    :return:
    """
