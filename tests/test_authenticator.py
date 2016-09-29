import pytest
import urlparse

from dartclient.core import create_basic_authenticator


@pytest.mark.integration_test
def test_authenticator_with_client(integration_test_client,
                                   integration_test_api_url,
                                   integration_test_api_key,
                                   integration_test_secret_key):

    integration_test_client.swagger_spec.http_client.authenticator = create_basic_authenticator(
        host=urlparse.urlparse(integration_test_api_url).hostname,
        username=integration_test_api_key,
        password=integration_test_secret_key)

    response = integration_test_client.Datastore.listDatastores().result()
    assert hasattr(response, 'total')
