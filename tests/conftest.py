import os

import pytest

from dartclient.core import create_client
from dartclient.core import ModelFactory


@pytest.fixture(scope="session")
def github_origin_url():
    return 'https://raw.githubusercontent.com/RetailMeNotSandbox/dart/master/src/python/dart/web/api/swagger.yaml'


@pytest.fixture(scope="session")
def client(github_origin_url):
    return create_client(origin_url=github_origin_url)


@pytest.fixture(scope="session")
def model_defaults():
    return {
        'on_failure_email': ['test1@example.com'],
        'on_started_email': ['test2@example.com'],
        'on_success_email': ['test3@example.com'],
        'tags': ['model_factory']
    }


@pytest.fixture(scope="session")
def model_factory(client, model_defaults):
    return ModelFactory(client, **model_defaults)


@pytest.fixture(scope="session")
def integration_test_api_key():
    return os.environ['DART_API_KEY']


@pytest.fixture(scope="session")
def integration_test_secret_key():
    return os.environ['DART_SECRET_KEY']


@pytest.fixture(scope="session")
def integration_test_url():
    return os.environ['DART_URL']


@pytest.fixture(scope="session")
def integration_test_api_url(integration_test_url):
    return '%s/api/1' % (integration_test_url,)


@pytest.fixture(scope="session")
def integration_test_spec_url(integration_test_api_url):
    return '%s/swagger.json' % (integration_test_api_url,)


@pytest.fixture(scope="session")
def integration_test_client(integration_test_spec_url):
    return create_client(integration_test_spec_url)
