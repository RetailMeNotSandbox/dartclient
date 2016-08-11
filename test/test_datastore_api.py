from dartclient import *


def test_list_datastores():
    response = DatastoreApi.listDatastores(filters='["name = xavier_emr_cluster"]', limit=20, offset=0, **DEBUG_ARGS).result()
    print(response)


test_list_datastores()