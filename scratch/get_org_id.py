import httpx
from sys import stderr
from multiprocessing import cpu_count
from math import ceil
import base64

import asyncio
from aiomultiprocess import Pool
from typing import List
from pathlib import Path
from pydantic import BaseModel
import aiofiles
from urllib.parse import urlparse


url = "https://rehket-big-load-test.my.salesforce.com"
token = "00D5f00000hL0FLMKzov1A0Cg3A0s2CzDXVLd25tvpb"


def get_org_id(
    base_url: str,
    version: str,
):
    """
    Retrieves an organization ID from the salesforce instance.
    :param base_url: The SalesForce Domain URL
    :param version: The api version for the API.
    :return:
    """
    query_path = f"services/data/v{version}/"
    data = httpx.get(
        f"{base_url}/{query_path}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    if data.status_code != 200:
        print(data.content.decode(), file=stderr)
    return data.json().get("identity").split("/")[-2]


if __name__ == "__main__":
    print(get_org_id(base_url=url, version="53.0"))
