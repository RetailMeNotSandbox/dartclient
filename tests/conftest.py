import os
import pytest

from dartclient.core import create_client


@pytest.fixture(scope="session")
def dart_api_url():
    return 'http://%s/api/1' % (os.environ['DART_HOST'],)


@pytest.fixture(scope="session")
def dart_client(dart_api_url):
    return create_client(api_url=dart_api_url)
