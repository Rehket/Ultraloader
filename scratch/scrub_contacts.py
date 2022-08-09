from os import getenv

from httpx import Client
from rich import print
import string
import random
from typing import List
import csv
letters = string.ascii_lowercase

instance_url= "https://officedepot--oduat.my.salesforce.com"
token="00D590000004ea0!AQ8AQBjKmgt4szdYN0BAIjs33Hryevr16yM8685WalmWo.NmqpeR2ciSvriRWlnG..wt_xXpRdArM2re3rICfxveKl.k_Zqh"




client = Client(
    base_url=instance_url,
    headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    timeout=60
)

response = client.get(url="/services/data/v53.0/query/?q=SELECT+id+from+lead+where+NOT+EMAIL+LIKE+'%@yopmail.com'")

if response.status_code != 200:
    print(response.text)

done = response.json().get("done")
record_list: List[dict] = []
for contact in response.json().get("records"):
    contact["Email"] = ''.join(random.choice(letters) for i in range(15)) + "@yopmail.com"
    record_list.append(contact)

next_url = response.json().get("nextRecordsUrl")

while not done:
    next_response = client.get(url=next_url)
    for contact in next_response.json().get("records"):
        contact["Email"] = ''.join(random.choice(letters) for i in range(15)) + "@yopmail.com"
        record_list.append(contact)
    done = next_response.json().get("done")
    next_url = next_response.json().get("nextRecordsUrl")
    print(len(record_list))

print(len(record_list))

with open("data/leads_scrambled.csv", "w") as contacts_out:
    writer = csv.DictWriter(contacts_out, fieldnames=["Id", "Email"], dialect=csv.QUOTE_ALL, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(record_list)