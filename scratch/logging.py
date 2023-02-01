#!/usr/bin/env python3.10

import typer
import httpx
import os
import json
import sys
import datetime

app = typer.Typer()


@app.command()
def upsert_simple_job(
    job_name: str,
    token: str = os.getenv("SFDC_BEARER_TOKEN"),
    base_url: str = os.getenv("SFDC_BEARER_TOKEN"),
    sfdc_api_version: str = os.getenv("SFDC_API_VERSION", "54.0"),
) -> dict:
    try:
        response = httpx.patch(
            f"{base_url}/services/data/v{sfdc_api_version}/sobjects/Simple_Job__c/Name/{job_name.upper()}",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            data={},
        )
        print(json.dumps(response.json(), indent=2))
        return response.json()
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise typer.Exit(-1)


@app.command()
def start_simple_job_run(

    job_name: str,
    user: str = None,
    token: str = os.getenv("SFDC_BEARER_TOKEN"),
    instance_url: str = os.getenv("SFDC_INSTANCE_URL"),
    sfdc_api_version: str = os.getenv("SFDC_API_VERSION", "54.0"),
) -> dict:
    try:
        response = httpx.patch(
            f"{instance_url}/services/data/v{sfdc_api_version}/sobjects/Simple_Job_Run__c/Job_Run_Id__c/",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            data={
                "Job__r": {"Name": job_name},
                "Start_Time__c": datetime.datetime.now().isoformat(),
                "User_Identifier__c": user,
                "Job_State__c": "RUNNING",
            },
        )
        print(json.dumps(response.json(), indent=2))
        return response.json()
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise typer.Exit(-1)


@app.command()
def end_simple_job_run(
    job_id: str,
    job_state: str,
    token: str = os.getenv("SFDC_BEARER_TOKEN"),
    instance_url: str = os.getenv("SFDC_INSTANCE_URL"),
    sfdc_api_version: str = os.getenv("SFDC_API_VERSION", "54.0"),
) -> dict:
    try:
        response = httpx.patch(
            f"{instance_url}/services/data/v{sfdc_api_version}/sobjects/Simple_Job_Run__c/Job_Run_Id__c/{job_id}",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            data={
                "Job__r": {"Name": job_name},
                "Actual_End_Time__c": datetime.datetime.now().isoformat(),
                "Job_State__c": job_state.upper(),
            },
        )
        print(json.dumps(response.json(), indent=2))
        return response.json()
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise typer.Exit(-1)


@app.command()
def add_log_for_job_run(
    job_id: str,
    job_name: str,
    message: str,
    message_type: str = "INFO",
    token: str = os.getenv("SFDC_BEARER_TOKEN"),
    instance_url: str = os.getenv("SFDC_INSTANCE_URL"),
    sfdc_api_version: str = os.getenv("SFDC_API_VERSION", "54.0"),
) -> dict:
    try:
        response = httpx.patch(
            f"{instance_url}/services/data/v{sfdc_api_version}/sobjects/Simple_Job_Log__c/",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            data={
                "Simple_Job__r": {"Name": job_name.upper()},
                "Simple_Job_Run__c": job_id,
                "Message__c": message,
                "Log_Type__c": message_type.upper(),
            },
        )
        print(json.dumps(response.json(), indent=2))
        return response.json()
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise typer.Exit(-1)


if __name__ == "__main__":

    app()
