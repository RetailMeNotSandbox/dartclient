import pytest

from bravado.client import SwaggerClient
from dartclient.core import create_sync_manager, ModelFactory, SyncManager


# Set HTTP_DEBUG=true (case sensitive) to log HTTP traffic
import os
if os.environ.get('HTTP_DEBUG') == 'true':
    import logging
    # These two lines enable debugging at httplib level (requests->urllib3->http.client)
    # You will see the REQUEST, including HEADERS and DATA, and RESPONSE with HEADERS but without DATA.
    # The only thing missing will be the response.body which is not logged.
    try:
        import http.client as http_client
    except ImportError:
        # Python 2
        import httplib as http_client
    http_client.HTTPConnection.debuglevel = 1

    # You must initialize logging, otherwise you'll not see debug output.
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True


def test_create_sync_manager_no_args():
    with pytest.raises(RuntimeError):
        create_sync_manager()


def test_create_sync_manager_origin_url(github_origin_url):
    sync_manager = create_sync_manager(origin_url=github_origin_url)
    assert isinstance(sync_manager, SyncManager)
    assert isinstance(sync_manager.client, SwaggerClient)
    assert isinstance(sync_manager.model_factory, ModelFactory)


def test_create_sync_manager_with_objs(client, model_factory):
    sync_manager = create_sync_manager(
        client=client, model_factory=model_factory)
    assert isinstance(sync_manager, SyncManager)
    assert sync_manager.client == client
    assert sync_manager.model_factory == model_factory


@pytest.mark.integration_test
def test_synchronization(integration_test_client, clean=False):
    # Build the Dart model
    model = TestDartModel(client=integration_test_client)
    try:
        # Make sure that Dart is clean
        model.clean()
        model.check_clean()

        # Synchronize and validate the model
        model.synchronize()
        model.validate()

        # Synchronize and validate the model again (exercises the update paths)
        model.synchronize()
        model.validate()
    finally:
        # Clean a final time
        if clean:
            model.clean()


class TestDartModel(object):
    """
    This is an example of how the Dartclient API is intended to be used.
    It includes some extra features (check_clean and validate) that clients
    won't necessarily need to implement.
    """
    DEFAULTS = {
        'on_failure_email': ['team@example.com'],
        'on_started_email': ['team@example.com'],
        'on_success_email': ['team@example.com'],
        'tags': ['dartclient', 'test']
    }

    DATASTORE1_NAME = 'dartclient_test_datastore1'
    DATASTORE1_STATE = 'INACTIVE'
    WORKFLOW1_NAME = 'dartclient_test_workflow1'
    ACTION1_NAME = 'dartclient_test_action1'
    ACTION2_NAME = 'dartclient_test_action2'
    TRIGGER1_NAME = 'dartclient_test_trigger1'
    DATASET1_NAME = 'dartclient_test_dataset1'

    DATASTORE2_NAME = 'dartclient_test_datastore2'
    DATASTORE2_STATE = 'ACTIVE'
    WORKFLOW2_NAME = 'dartclient_test_workflow2'
    SUBSCRIPTION1_NAME = 'dartclient_test_subscription1'
    ACTION3_NAME = 'dartclient_test_action3'
    ACTION4_NAME = 'dartclient_test_action4'
    TRIGGER2_NAME = 'dartclient_test_trigger2'

    def __init__(self, client):
        self.client = client
        self.sync_manager = create_sync_manager(
            client=client,
            model_defaults=self.DEFAULTS)

    def clean(self):
        self.sync_manager.clean_datastore(self.sync_manager.find_datastore(
            self.DATASTORE1_NAME, self.DATASTORE1_STATE))
        self.sync_manager.clean_datastore(self.sync_manager.find_datastore(
            self.DATASTORE2_NAME, self.DATASTORE2_STATE))
        self.sync_manager.clean_subscription(
            self.sync_manager.find_subscription(self.SUBSCRIPTION1_NAME))
        self.sync_manager.clean_dataset(
            self.sync_manager.find_dataset(self.DATASET1_NAME))

    def synchronize(self):
        ds1 = self.sync_manager.sync_datastore(
            self.DATASTORE1_NAME, self.DATASTORE1_STATE, self.define_datastore1)
        wf1 = self.sync_manager.sync_workflow(
            self.WORKFLOW1_NAME, ds1, self.define_workflow1)
        self.sync_manager.sync_action(
            self.ACTION1_NAME, wf1, self.define_action1)
        self.sync_manager.sync_action(
            self.ACTION2_NAME, wf1, self.define_action2)
        self.sync_manager.sync_trigger(
            self.TRIGGER1_NAME, wf1, self.define_trigger1)
        dat1 = self.sync_manager.sync_dataset(
            self.DATASET1_NAME, self.define_dataset1)
        sub1 = self.sync_manager.sync_subscription(
            self.SUBSCRIPTION1_NAME, dat1, self.define_subscription1)

        ds2 = self.sync_manager.sync_datastore(
            self.DATASTORE2_NAME, self.DATASTORE2_STATE, self.define_datastore2)
        wf2 = self.sync_manager.sync_workflow(
            self.WORKFLOW2_NAME, ds2, self.define_workflow2)
        self.sync_manager.sync_action(
            self.ACTION3_NAME, wf2, self.define_action3, dataset=dat1)
        self.sync_manager.sync_action(
            self.ACTION4_NAME, wf2, self.define_action4, subscription=sub1)
        self.sync_manager.sync_trigger(
            self.TRIGGER2_NAME, wf2, self.define_trigger2, subscription=sub1)

    def define_datastore1(self, datastore):
        datastore.data.args = {
            "data_to_freespace_ratio": 0.5,
            "dry_run": True,
            "instance_count": 1,
            "instance_type": "m3.large",
            "release_label": "emr-4.3.0"
        }
        datastore.data.concurrency = 1
        datastore.data.engine_name = 'emr_engine'
        return datastore

    def define_workflow1(self, workflow):
        workflow.data.concurrency = 1
        workflow.data.engine_name = 'emr_engine'
        return workflow

    def define_action1(self, action):
        action.data.action_type_name = 'start_datastore'
        action.data.engine_name = 'emr_engine'
        action.data.order_idx = 0
        action.data.state = 'TEMPLATE'
        return action

    def define_action2(self, action):
        action.data.action_type_name = 'terminate_datastore'
        action.data.engine_name = 'emr_engine'
        action.data.order_idx = 1
        action.data.state = 'TEMPLATE'
        return action

    def define_trigger1(self, trigger):
        trigger.data.args = {
            'cron_pattern': '0 0 * * ? *'
        }
        trigger.data.trigger_type_name = 'scheduled'
        return trigger

    def define_dataset1(self, dataset):
        column1 = self.client.get_model('Column')()
        column1.data_type = 'VARCHAR'
        column1.name = 'column1'

        column2 = self.client.get_model('Column')()
        column2.data_type = 'DATETIME'
        column2.name = 'column2'

        dataset.data.columns = [column1, column2]
        dataset.data.compression = 'BZ2'
        dataset.data.data_format.file_format = 'TEXTFILE'
        dataset.data.data_format.row_format = 'DELIMITED'
        dataset.data.load_type = 'INSERT'
        dataset.data.location = 's3://example-bucket/path'
        dataset.data.table_name = 'table1'
        # dataset.data.user_id = 'user1'
        return dataset

    def define_datastore2(self, datastore):
        datastore.data.args = {
            'action_sleep_time_in_seconds': 5
        }
        datastore.data.concurrency = 1
        datastore.data.engine_name = 'no_op_engine'
        return datastore

    def define_workflow2(self, workflow):
        workflow.data.concurrency = 1
        workflow.data.engine_name = 'no_op_engine'
        return workflow

    def define_action3(self, action):
        action.data.action_type_name = 'fake_load_dataset'
        action.data.engine_name = 'no_op_engine'
        action.data.order_idx = 0
        action.data.state = 'TEMPLATE'
        return action

    def define_action4(self, action):
        action.data.action_type_name = 'consume_subscription'
        action.data.engine_name = 'no_op_engine'
        action.data.order_idx = 1
        action.data.state = 'TEMPLATE'
        return action

    def define_subscription1(self, subscription):
        subscription.data.s3_path_start_prefix_inclusive = 's3://s3-rpt-uss-dat-warehouse/dart-tests/tst/'
        subscription.data.state = 'INACTIVE'
        return subscription

    def define_trigger2(self, trigger):
        trigger.data.args = {
            'unconsumed_data_size_in_bytes': 20000
        }
        trigger.data.name = self.TRIGGER2_NAME
        trigger.data.trigger_type_name = 'subscription_batch'
        return trigger

    def check_clean(self):
        """
        Assert that the entities do not exist in Dart.
        :return:
        """
        assert self.sync_manager.find_datastore(
            self.DATASTORE1_NAME, self.DATASTORE1_STATE) is None
        assert self.sync_manager.find_datastore(
            self.DATASTORE2_NAME, self.DATASTORE2_STATE) is None
        assert self.sync_manager.find_dataset(self.DATASET1_NAME) is None
        assert self.sync_manager.find_subscription(
            self.SUBSCRIPTION1_NAME) is None

    def validate(self):
        """
        Validate that the entities in Dart are what this class defined them to be.
        :return:
        """
        ds1 = self.validate_datastore1()
        wf1 = self.validate_workflow1(ds1)
        self.validate_action1(wf1)
        self.validate_action2(wf1)
        self.validate_trigger1(wf1)
        dat1 = self.validate_dataset1()
        sub1 = self.validate_subscription1(dat1)

        ds2 = self.validate_datastore2()
        wf2 = self.validate_workflow2(ds2)
        self.validate_action3(wf2, dat1)
        self.validate_action4(wf2, sub1)
        self.validate_trigger2(wf2, sub1)

    def validate_datastore1(self):
        datastore = self.sync_manager.find_datastore(
            self.DATASTORE1_NAME, self.DATASTORE1_STATE)
        self.validate_object(datastore)

        assert datastore.data.args == {
            "data_to_freespace_ratio": 0.5,
            "dry_run": True,
            "instance_count": 1,
            "instance_type": "m3.large",
            "release_label": "emr-4.3.0"
        }
        assert datastore.data.concurrency == 1
        assert datastore.data.engine_name == 'emr_engine'
        assert datastore.data.name == self.DATASTORE1_NAME
        return datastore

    def validate_workflow1(self, datastore):
        workflow = self.sync_manager.find_workflow(
            self.WORKFLOW1_NAME, datastore)
        self.validate_object(workflow)

        assert workflow.data.concurrency == 1
        assert workflow.data.engine_name == 'emr_engine'
        assert workflow.data.name == self.WORKFLOW1_NAME
        return workflow

    def validate_action1(self, workflow):
        action = self.sync_manager.find_action(self.ACTION1_NAME, workflow)
        self.validate_object(action)

        assert action.data.action_type_name == 'start_datastore'
        assert action.data.engine_name == 'emr_engine'
        assert action.data.name == self.ACTION1_NAME
        assert action.data.order_idx == 0
        assert action.data.state == 'TEMPLATE'

    def validate_action2(self, workflow):
        action = self.sync_manager.find_action(self.ACTION2_NAME, workflow)
        self.validate_object(action)

        assert action.data.action_type_name == 'terminate_datastore'
        assert action.data.engine_name == 'emr_engine'
        assert action.data.name == self.ACTION2_NAME
        assert action.data.order_idx == 1
        assert action.data.state == 'TEMPLATE'

    def validate_trigger1(self, workflow):
        trigger = self.sync_manager.find_trigger(self.TRIGGER1_NAME, workflow)
        self.validate_object(trigger)

        assert trigger.data.args == {
            'cron_pattern': '0 0 * * ? *'
        }
        assert trigger.data.name == self.TRIGGER1_NAME
        assert trigger.data.trigger_type_name == 'scheduled'

    def validate_dataset1(self):
        dataset = self.sync_manager.find_dataset(self.DATASET1_NAME)
        self.validate_object(dataset)

        assert len(dataset.data.columns) == 2
        column1 = dataset.data.columns[0]
        assert column1.data_type == 'VARCHAR'
        assert column1.name == 'column1'
        column2 = dataset.data.columns[1]
        assert column2.data_type == 'DATETIME'
        assert column2.name == 'column2'
        assert dataset.data.compression == 'BZ2'
        assert dataset.data.data_format.file_format == 'TEXTFILE'
        assert dataset.data.data_format.row_format == 'DELIMITED'
        assert dataset.data.load_type == 'INSERT'
        assert dataset.data.location == 's3://example-bucket/path'
        assert dataset.data.name == self.DATASET1_NAME
        assert dataset.data.table_name == 'table1'
        # assert dataset.data.user_id == 'user1'
        return dataset

    def validate_datastore2(self):
        datastore = self.sync_manager.find_datastore(
            self.DATASTORE2_NAME, self.DATASTORE2_STATE)
        self.validate_object(datastore)

        assert datastore.data.concurrency == 1
        assert datastore.data.engine_name == 'no_op_engine'
        assert datastore.data.name == self.DATASTORE2_NAME
        assert datastore.data.state == 'ACTIVE'
        return datastore

    def validate_workflow2(self, datastore):
        workflow = self.sync_manager.find_workflow(
            self.WORKFLOW2_NAME, datastore)
        self.validate_object(workflow)

        assert workflow.data.concurrency == 1
        assert workflow.data.engine_name == 'no_op_engine'
        assert workflow.data.name == self.WORKFLOW2_NAME
        return workflow

    def validate_action3(self, workflow, dataset):
        action = self.sync_manager.find_action(self.ACTION3_NAME, workflow)
        self.validate_object(action)

        assert action.data.args == {
            'dataset_id': dataset.id
        }
        assert action.data.action_type_name == 'fake_load_dataset'
        assert action.data.engine_name == 'no_op_engine'
        assert action.data.name == self.ACTION3_NAME
        assert action.data.order_idx == 0
        assert action.data.state == 'TEMPLATE'

    def validate_action4(self, workflow, subscription):
        action = self.sync_manager.find_action(self.ACTION4_NAME, workflow)
        self.validate_object(action)

        assert action.data.args == {
            'subscription_id': subscription.id
        }
        assert action.data.action_type_name == 'consume_subscription'
        assert action.data.engine_name == 'no_op_engine'
        assert action.data.order_idx == 1
        assert action.data.state == 'TEMPLATE'
        return action

    def validate_subscription1(self, dataset):
        subscription = self.sync_manager.find_subscription(
            self.SUBSCRIPTION1_NAME)
        self.validate_object(subscription)

        assert subscription.data.name == self.SUBSCRIPTION1_NAME
        assert subscription.data.dataset_id == dataset.id
        assert subscription.data.s3_path_start_prefix_inclusive == 's3://s3-rpt-uss-dat-warehouse/dart-tests/tst/'
        # assert subscription.data.state == 'INACTIVE'
        return subscription

    def validate_trigger2(self, workflow, subscription):
        trigger = self.sync_manager.find_trigger(self.TRIGGER2_NAME, workflow)
        self.validate_object(trigger)

        assert trigger.data.args == {
            'subscription_id': subscription.id,
            'unconsumed_data_size_in_bytes': 20000
        }
        assert trigger.data.name == self.TRIGGER2_NAME
        assert trigger.data.trigger_type_name == 'subscription_batch'
        return trigger

    def validate_object(self, obj):
        assert obj.created is not None and isinstance(obj.created, basestring)
        assert obj.data is not None
        assert obj.id is not None and isinstance(obj.id, basestring)
        assert obj.updated is not None and isinstance(obj.updated, basestring)
        assert obj.version_id is not None and isinstance(obj.version_id, int)
        if hasattr(obj.data, 'on_failure_email'):
            assert obj.data.on_failure_email == self.DEFAULTS[
                'on_failure_email']
        if hasattr(obj.data, 'on_started_email'):
            assert obj.data.on_started_email == self.DEFAULTS[
                'on_started_email']
        if hasattr(obj.data, 'on_success_email'):
            assert obj.data.on_success_email == self.DEFAULTS[
                'on_success_email']
        assert set(self.DEFAULTS['tags']) <= set(obj.data.tags)
