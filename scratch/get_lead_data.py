import json

from mimesis import Address, Finance, Person
from mimesis.builtins import USASpecProvider
from mimesis.schema import Field, Schema
from csv import DictWriter


def user_description():
    f = Field('en', providers=[USASpecProvider])

    return {
        'FirstName': f('first_name'),
        'LastName': f('last_name'),
        'Email': f('person.email'),
        'Company': f('company'),
        'Street': f('address'),
        'City': f('city'),
        'PostalCode': f('zip_code'),
        'Phone': person.telephone(mask='###-###-###')


    }
person = Person("en")
company = Finance("en")
addr = Address("en")
schema = Schema(schema=user_description)
data = []
for i in range(0, 900000):
    data.append({
        'FirstName': person.first_name(),
        'LastName': person.last_name(),
        'Email': person.email(),
        'Company': company.company(),
        'Street': addr.address(),
        'City': addr.city(),
        'State': addr.state(abbr=True),
        'PostalCode': addr.zip_code(),
        'Phone': person.telephone(mask='###-###-###')


    })


print(data[0:1])

with open("data/LeadsOut.csv", "w", newline='') as leads_out:
    writer = DictWriter(leads_out, list(data[0].keys()), lineterminator='\n')
    writer.writeheader()
    writer.writerows(data)
