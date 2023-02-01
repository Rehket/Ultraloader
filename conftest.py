import pytest
from tempfile import TemporaryDirectory
import pathlib
from unittest.mock import patch


@pytest.fixture
def get_temp_home():
    """
    This fixture replaces the home directory with a temporary directory that exists for the lifecycle of a test.
    Referenced: https://stackoverflow.com/questions/48864027/how-do-i-patch-the-pathlib-path-exists-method
    :return:
    """
    with TemporaryDirectory(dir=pathlib.Path().home()) as temp_dir:
        with patch.object(pathlib.Path, "home") as mock_home:
            mock_home.return_value = pathlib.Path(temp_dir)
            yield temp_dir
