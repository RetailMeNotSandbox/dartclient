import os
import pytest

from dartclient.core import create_client, ModelFactory


@pytest.fixture(scope="session")
def client():
    return create_client()


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
def integration_test_api_url():
    return 'https://%s/api/1' % (os.environ['DART_HOST'],)


@pytest.fixture(scope="session")
def integration_test_client(integration_test_api_url):
    return create_client(api_url=integration_test_api_url)
