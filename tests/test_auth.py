from ultra.auth import _get_or_create_secret
from ultra.exceptions import SalesForceAuthException, SalesForceUserException
import pytest
from ultra.auth import SalesforceCred
from pathlib import Path
from cryptography.fernet import Fernet
import os

TEST_CREDS = {
    "SFDC_USERNAME": "test@salesforce.com",
    "SFDC_CLIENT_ID": "3MwDP8_5DfNOLW29.CAgn",
    "SFDC_CLIENT_SECRET": "B0DDD356365318990DF0031D3565463222457577FF432DC33E1",
    "SFDC_ENVIRONMENT": "sandbox",
    "SFDC_SKIP_FIELDS": "",
    "SFDC_API_VERSION": "52.0",
}


@pytest.fixture()
def get_sfdc_test_credential():
    """
    Never put real credential in code.
    The Credentials below are all used specifically for testing and are only used in the scope of the tests.
    They are not used for and production or non production systems.
    :return:
    """
    yield SalesforceCred(
        username="test@salesforce.com",
        client_id="3MwDP8_5DfNOLW29.CAgn",
        client_secret="B0DDD356365318990DF0031D3565463222457577FF432DC33E1",
        private_key="""-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEA3hXnrJcYRgMY3QnEHZDAiQM05kj4RkvRdxNvFXZFYPBKEDmM
z2cMNenFax2YAPTEcy8PZNDMDNAZTUVr1yvFwl5eWrRLKVOboVRSby6KLR9i+hf0
jWH6MJ9JRSN7ldgyaOG4TtU+9XqlzsWyQpG0rebnjOReDnQouhR3uEAzYTi4MZUI
cgDwgdrg11FgT9XoIXR8eKHHprthKMICBxqoe07UVu41mw6+BZVwfDK1vIJJhKZx
hYqZG/YV2T3qhJv3MTrMWkXSFsUWuNnGH3mQlyd3cDEndW6niPmfsJP+kW7MVPpo
o6ncYhTcIVjj8FxhVfmnToEaXsRCJSSb2VAPSQIDAQABAoIBAQConJ7pVUnzldRh
tVF1VPoozAt0r3/39wZQTqvu8n2YLLc+fEMhEE6f/B2LxssqJquj+93HIBNbwv8C
wKswkSYy/OzMFshd1wF4yglQKlh0C7lcwaVFVlbaZYT+fgMNcDhNktoY2CRVwclz
JHvDuNYhlgbE3HlgpeZp6LJAbFXWAA0T3dbSudv6pYlib0+cZhHAwOYSUVp80t4G
e75GtCXtZslRGFqho67oO2LuIWR4Gm6VTmi+BAtWg3RVkHYc0bzrUW4dVzTms/U4
FaTF1fwQOf2R4cLuduoYD3EB7EzjdVeah+c9KK3RYW0J99KytPVvW5WHjJsyCfUw
GmyGaISBAoGBAPJe8qICKACVNOAhPnelMJUl5vuPPuU1lnkTwuXJJEFC9GSrypLn
hg8sUQ23LGYmx2t0UkTuo1Zwh8QLI3Z35EJH6x7lUkt1dnSt25g1MFEOPltdpDNG
hDc5qoQjfi3cJUYs0zc+c1jZv4FXU03v3L6o9xCSysCIGWKuBzFZAslxAoGBAOqS
8IyZghX9XKZ5H1/9tAhusyKRDKuyvbwMnIHuRkVg7FBgkGWvzVsQD1cJTrf1g9fd
iuF5sO1nXRRVHUzX4mLGtuT4/bOTzA7tLFNc+tVsUxz9Ko/NlVe03mAE2qyl5wcb
VEJc/y3srNkzK8YdhKauoXTzUZ5CNnSqO4bIvPdZAoGAPE6ZyuwEbhiyPm6nL3gf
/Yyfy8pZ2Qibd2cOYMTKy55QV1hToHgRaGcbh/EjEAvoJPmdmt9SLyjy7zniShEM
790bSOXAs1v37JJyCI57hj+oDm3fdI7ASUmE/zohpGGZuLtgludoJhyQRSuoY8Ui
RSKfzzTnVD7JKj+pNIM0aVECgYEAoz4u+Sm7ReJUH+Ya14qPR28CawPWdYamU7/T
CvjvHkkdK7KHyrxVhRHgGwn3Jj5NymP6yn8NialtNUEmatuySFtR3HcK13X7hEe5
mSoTxYLlND9a50iCrc7JErlOFOg/psp2ENj3HNagU2cxB2ZzciwFYIYUPraFfxsY
4evA1cECgYBxxHwfP5QKQIrs8ER2p5cmyc0P2Zf3b05RkipYzBVJ5pmDQHACluSd
T2GOAAhjJdk3kceZHSq0I9Sa3etM7qxshBfKBVlpVndWd5qLgcGrY+D3cgr9lI1J
Bsli958yUvGt6vUT2uGdnm10HA4UTOcX9oh9Q0aTUeHrcr7PljHs1Q==
-----END RSA PRIVATE KEY-----""",
        api_version="53.0",
        environment="sandbox",
    )


class TestSalesForceAuth:
    def test_get_or_create_secret(self, get_temp_home):
        """
        Test to validate that a secret key is generated and readable.

        The get_temp_home fixture is used to mock the home environment.
        :return:
        """

        # On the first call of the function, a new key file should be generated.
        fernet = _get_or_create_secret()
        assert fernet is not None
        # The default path for the file should be ~/.ultra/.ultra.key
        key_path = Path(Path().home(), ".ultra", ".ultra.key")

        # The file should exist.
        assert Path(key_path).exists()

        # On a Windows system, the file should be flagged as hidden.
        if os.name == "nt":
            assert os.stat(key_path).FILE_ATTRIBUTE_HIDDEN
        else:
            # On linux, hidden files are denoted with a leading `.`
            assert key_path.name.startswith(".")

        # Verify we can read the key data and use it to decrypt data.
        with open(key_path, "rb") as filekey:
            key = filekey.read()
            test_fernet = Fernet(key)
            assert (
                test_fernet.decrypt(token=fernet.encrypt(b"some Data")) == b"some Data"
            )
