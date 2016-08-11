from dartclient import *


def test_list_actions():
    response = ActionApi.listActions(filters='["name ~ xavier"]', limit=20, offset=0, **DEBUG_ARGS).result()
    print(response)


test_list_actions()