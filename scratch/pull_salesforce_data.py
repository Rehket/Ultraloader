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


class Batch(BaseModel):
    job_id: str
    batch_start: int
    batch_size: int
    api_version: str


url = "https://rehket-big-load-test.my.salesforce.com"
token = "00D5f000005vXXtnf.HdMlpHae1A0Cg3A0s2CzDXVLd25tvpb"


def get_query_job(job_id: str, version: str):
    query_path = f"services/data/v{version}/jobs/query/{job_id}"
    data = httpx.get(
        f"{url}/{query_path}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    if data.status_code != 200:
        print(data.content.decode(), file=stderr)
    return data.json()


def create_query_job(query: str, version: str):
    query_path = f"services/data/v{version}/jobs/query"

    body = {"operation": "query", "query": query}

    data = httpx.post(
        f"{url}/{query_path}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        json=body,
    )
    if data.status_code != 200 or data.status_code != 201:
        print(data.content.decode(), file=stderr)
    return data.json()


def get_query_data(job_id: str, locator: int, max_records: int, version: str):
    query_path = f"services/data/v{version}/jobs/query/{job_id}/results"

    data = httpx.get(
        f"{url}/{query_path}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        params={
            "maxRecords": max_records,
            "locator": base64.b64encode(str(locator).encode()).decode(),
        },
    )
    if data.status_code != 200 or data.status_code != 201:
        print(data.content.decode(), file=stderr)

    return data.content.decode()


async def a_get_query_data(
    batch: Batch,
    data_directory: str = "./data"
):
    query_path = f"services/data/v{batch.api_version}/jobs/query/{batch.job_id}/results"

    async with httpx.AsyncClient() as client:
        data = await client.get(
            f"{url}/{query_path}",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            params={
                "maxRecords": batch.batch_size,
                "locator": base64.b64encode(str(batch.batch_start).encode()).decode(),
            },
        )
    if data.status_code != 200 or data.status_code != 201:
        print(data.content.decode(), file=stderr)

    salesforce_domain_path = urlparse(url=url).netloc.split(".")[0]
    data_directory = Path(data_directory, salesforce_domain_path)
    data_directory.mkdir(exist_ok=True)
    file_path = Path(data_directory, f"{batch.job_id}_{batch.batch_start:012d}.csv")
    async with aiofiles.open(file_path, mode="w") as file_out:
        await file_out.write(data.content.decode())

    return data.content.decode()


async def pull_batches(lots: List[Batch]):

    async with Pool() as pool:
        async for result in pool.map(a_get_query_data, lots):
            print(result)



if __name__ == "__main__":
    # print(create_query_job(query="SELECT Id FROM LEAD WHERE ID=null", version="52.0"))
    # exit()
    job_data = get_query_job(job_id="7505f000002Xvzq", version="52.0")
    record_count = job_data.get("numberRecordsProcessed")
    if record_count == 0:
        print("Record Count is 0, No results to process", file=stderr)
        exit()

    batches_size = ceil(job_data.get("numberRecordsProcessed") / cpu_count())

    print(batches_size)

    lots = [
        Batch(
            batch_start=i,
            batch_size=batches_size,
            job_id="7505f000002Xvzq",
            api_version="53.0",
        )
        for i in range(0, job_data.get("numberRecordsProcessed"), batches_size)
    ]

    print(lots)

    asyncio.run(pull_batches(lots=lots))
    # for index, lot in enumerate(lots):
    #     print(lot)
    #     print(index)
    #     with open(f"small_pull_{index}.csv", "w") as small:
    #         small.write(get_query_data(job_id="7505f000002XvzqAAC", locator=lot[0], max_records=batches_size, version="53.0"))
    #
    #
    # with open("full_pull.csv", "w") as full:
    #     full.write(
    #         get_query_data(
    #             job_id="7505f000002XvzqAAC", locator=0, max_records=1000, version="53.0"
    #         )
    #     )




