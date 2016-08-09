from dartclient import *


def debug_callback(response, operation):
    print(response.text)


DEBUG_ARGS = {
    '_request_options': {
        'response_callbacks': [
            debug_callback
        ]
    }
}


def test_list_datastores():
    response = DatastoreApi.listDatastores(filters='["name ~ xavier"]', limit=20, offset=0, **DEBUG_ARGS).result()
    print(response)


test_list_datastores()