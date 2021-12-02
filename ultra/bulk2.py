import sys

import httpx
from sys import stderr
from multiprocessing import cpu_count
from math import ceil
import base64

import asyncio
from aiomultiprocess import Pool
from typing import List, Optional
from pathlib import Path
from pydantic import BaseModel
import aiofiles
from tenacity import (
    AsyncRetrying,
    RetryError,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from urllib.parse import urlparse
from sfjwt import CredentialModel, load_credentials
from file_operations import combine_files

from tempfile import gettempdir
import shutil


class Organization(BaseModel):
    org_id: str
    client_id: str
    base_url: str


class Batch(BaseModel):
    base_path: str
    job_id: str
    batch_start: int
    batch_size: int
    api_version: str
    object: str


def get_query_job(
    job_id: str,
    version: str,
    client: httpx.Client = None,
    credentials: CredentialModel = None,
):
    if credentials is None:
        credentials = load_credentials()
    if client is None:
        client = httpx.Client(
            base_url=credentials.instance_url,
            headers={
                "Authorization": f"Bearer {credentials.token}",
                "Accept": "application/json",
            },
        )
    query_path = f"services/data/v{version}/jobs/query/{job_id}"
    data = client.get(
        f"{query_path}",
    )
    if data.status_code != 200:
        print(data.content.decode(), file=stderr)
    return data.json()


def create_query_job(
    query: str,
    version: str,
    client: httpx.Client = None,
    credentials: CredentialModel = None,
):
    if credentials is None:
        credentials = load_credentials()
    if client is None:
        client = httpx.Client(
            base_url=credentials.instance_url,
            headers={
                "Authorization": f"Bearer {credentials.token}",
                "Accept": "application/json",
            },
        )

    query_path = f"services/data/v{version}/jobs/query"

    body = {"operation": "query", "query": query}

    data = client.post(
        f"{query_path}",
        json=body,
    )
    if data.status_code != 200 and data.status_code != 201:
        print(data.content.decode(), file=stderr)
    return data.json()


def get_query_data(
    job_id: str,
    locator: int,
    max_records: int,
    version: str,
    client: httpx.Client = None,
    credentials: CredentialModel = None,
):
    if credentials is None:
        credentials = load_credentials()
    if client is None:
        client = httpx.Client(
            base_url=credentials.instance_url,
            headers={
                "Authorization": f"Bearer {credentials.token}",
                "Accept": "application/json",
            },
        )
    query_path = f"/services/data/v{version}/jobs/query/{job_id}/results"

    data = client.get(
        f"{query_path}",
        params={
            "maxRecords": max_records,
            "locator": base64.b64encode(str(locator).encode()).decode(),
        },
    )
    if data.status_code != 200 and data.status_code != 201:
        print(data.content.decode(), file=stderr)

    return data.content.decode()


async def a_get_query_data(
    batch: Batch,
    data_directory: str = "./data",
    async_client: httpx.AsyncClient = None,
    credentials: CredentialModel = None,
):
    if credentials is None:
        credentials = load_credentials()
    if async_client is None:
        async_client = httpx.AsyncClient(
            base_url=credentials.instance_url,
            headers={
                "Authorization": f"Bearer {credentials.token}",
                "Accept": "application/json",
            },
        )

    query_path = (
        f"/services/data/v{batch.api_version}/jobs/query/{batch.job_id}/results"
    )

    try:
        async for attempt in AsyncRetrying(
            retry=retry_if_exception_type(httpx.ReadTimeout),
            stop=stop_after_attempt(10),
            wait=wait_exponential(multiplier=1, min=4, max=10),
        ):
            with attempt:
                data = await async_client.get(
                    f"{query_path}",
                    params={
                        "maxRecords": batch.batch_size,
                        "locator": base64.b64encode(
                            str(batch.batch_start).encode()
                        ).decode(),
                    },
                )
    except RetryError:
        print("Error occurred while downloading job data")
    finally:
        await async_client.aclose()
    if data.status_code != 200 and data.status_code != 201:
        print(data.content.decode(), file=stderr)

    salesforce_domain_path = urlparse(url=batch.base_path).netloc.split(".")[0]
    data_directory = Path(data_directory, salesforce_domain_path)
    data_directory.mkdir(exist_ok=True)
    file_path = Path(data_directory, f"{batch.job_id}_{batch.batch_start:012d}.csv")

    async with aiofiles.open(file_path, mode="w") as file_out:
        await file_out.write(data.content.decode())

    return batch.json(indent=2)


async def pull_batches(lots: List[Batch]):

    async with Pool() as pool:
        async for result in pool.map(a_get_query_data, lots):
            print(result)


def download_query_data(job_id: str, version: str = "53.0"):
    job_data = get_query_job(job_id=job_id, version=version)
    record_count = job_data.get("numberRecordsProcessed")
    credentials = load_credentials()
    if record_count == 0:
        print("Record Count is 0, No results to process", file=stderr)
        exit()

    batches_size = ceil(job_data.get("numberRecordsProcessed") / cpu_count())

    lots = [
        Batch(
            batch_start=i,
            batch_size=batches_size,
            job_id=job_id,
            api_version=version,
            base_path=credentials.instance_url,
            object=job_data.get("object"),
        )
        for i in range(0, job_data.get("numberRecordsProcessed"), batches_size)
    ]

    asyncio.run(pull_batches(lots=lots))


def get_job(
    job_id: str,
    version: str,
    client: httpx.Client = None,
    credentials: CredentialModel = None,
):
    if credentials is None:
        credentials = load_credentials()
    if client is None:
        client = httpx.Client(
            base_url=credentials.instance_url,
            headers={
                "Authorization": f"Bearer {credentials.token}",
                "Accept": "application/json",
            },
        )
    query_path = f"services/data/v{version}/jobs/ingest/{job_id}"
    data = client.get(
        f"{query_path}",
    )
    if data.status_code != 200:
        print(data.content.decode(), file=stderr)
    return data.json()


def create_ingest_job(
    object_name: str,
    operation: str,
    external_id_field_name: str,
    version: str,
    client: httpx.Client = None,
    credentials: CredentialModel = None,
):
    if credentials is None:
        credentials = load_credentials()
    if client is None:
        client = httpx.Client(
            base_url=credentials.instance_url,
            headers={
                "Authorization": f"Bearer {credentials.token}",
                "Accept": "application/json",
            },
        )

    query_path = f"services/data/v{version}/jobs/ingest"

    body = {"operation": operation, "object": object_name}
    if operation == "upsert":
        if external_id_field_name is None or external_id_field_name == "":
            raise RuntimeError(
                "external Id field name must be provided when performing upsert."
            )
        body["externalIdFieldName"] = external_id_field_name

    data = client.post(
        f"{query_path}",
        json=body,
    )
    if data.status_code != 200 and data.status_code != 201:
        print(data.content.decode(), file=stderr)
    return data.json()


def load_ingest_job_data(
    job_id: str,
    file_path: str,
    version: str,
    client: httpx.Client = None,
    credentials: CredentialModel = None,
):

    if credentials is None:
        credentials = load_credentials()
    if client is None:
        client = httpx.Client(
            base_url=credentials.instance_url,
            headers={
                "Authorization": f"Bearer {credentials.token}",
            },
        )
    client.headers.update({"Content-Type": "text/csv"})
    query_path = f"services/data/v{version}/jobs/ingest/{job_id}"

    with open(file_path, "r") as file_in:
        data = client.put(f"{query_path}/batches", content=file_in.read(), timeout=None)
        if data.status_code != 200 and data.status_code != 201:
            print(data.content.decode(), file=stderr)

    print(data.content.decode())
    print(f"Batch: {file_path} loaded.")
    client.headers.update({"Content-Type": "application/json"})
    payload = {
        "state": "UploadComplete" if data.status_code in (200, 201) else "Aborted"
    }
    print(payload)
    result = client.patch(
        f"{query_path}",
        json=payload,
        timeout=None,
    )
    return result.json()


def ingest_job_data_batches(
    object_name: str,
    operation: str,
    path_or_file: str,
    pattern: str,
    batch_size: int,
    version: str,
    external_id_field_name: str = None,
    working_directory: str = None,
    client: httpx.Client = None,
    credentials: CredentialModel = None,
):

    if working_directory is None:
        working_directory = Path(gettempdir(), object_name)
    if working_directory.exists() and working_directory.is_dir():
        # working_directory.rmdir()
        shutil.rmtree(working_directory)
    working_directory.mkdir(parents=True)

    if credentials is None:
        credentials = load_credentials()
    if client is None:
        client = httpx.Client(
            base_url=credentials.instance_url,
            headers={
                "Authorization": f"Bearer {credentials.token}",
                "Content-Type": "application/json",
            },
        )

    file_task = combine_files(
        path_or_file=path_or_file,
        pattern=pattern,
        output_directory=working_directory,
        file_size_limit=batch_size,
    )

    if file_task.status != "success":
        raise RuntimeError(f"Combining files failed: {file_task.message}")

    for file_path in file_task.payload:

        bulk_job = create_ingest_job(
            object_name=object_name,
            operation=operation.lower(),
            external_id_field_name=external_id_field_name,
            version=version,
            client=client,
            credentials=credentials,
        )

        load_ingest_job_data(
            job_id=bulk_job.get("id"),
            file_path=file_path,
            version=version,
            client=client,
            credentials=credentials,
        )
