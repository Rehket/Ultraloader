from os import getenv

from ultraloader.sfjwt import jwt_login
from httpx import Client
from rich import print
if __name__ == "__main__":
    private_key = getenv("SFDC_PRIVATE_KEY")
    consumer_id = getenv("SFDC_CONSUMER_ID")
    username = getenv("SFDC_USERNAME")
    environment = getenv("SFDC_ENVIRONMENT")

    url, token = jwt_login(
        consumer_id=consumer_id,
        username=username,
        private_key=private_key,
        environment=environment,
    )

    client = Client(
        base_url=url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )

    response = client.get(url="/services/data/v52.0/sobjects/Account/describe/")

    if response.status_code != 200:
        print(response.text)


    print(response.json().get("fields")[0].keys())
    print([field for field in response.json().get("fields") if field.get("updateable") is True])


