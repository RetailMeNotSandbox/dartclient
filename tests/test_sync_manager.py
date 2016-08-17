import pytest

from dartclient.core import create_sync_manager


@pytest.mark.integrationtest
def test_synchronization(dart_client):
    # Build the Dart model
    model = TestDartModel(client=dart_client)
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
    WORKFLOW1_NAME = 'dartclient_test_workflow1'
    ACTION1_NAME = 'dartclient_test_action1'
    ACTION2_NAME = 'dartclient_test_action2'
    TRIGGER1_NAME = 'dartclient_test_trigger1'
    DATASET1_NAME = 'dartclient_test_dataset1'

    def __init__(self, client):
        self.client = client
        self.sync_manager = create_sync_manager(
            client=client,
            model_defaults=self.DEFAULTS)

    def clean(self):
        """
        Delete all the entities in Dart associated with this model in
        the opposite order of creation.
        """
        self.sync_manager.delete_dataset_by_name(self.DATASET1_NAME)
        self.sync_manager.delete_trigger_by_name(self.TRIGGER1_NAME)
        self.sync_manager.delete_action_by_name(self.ACTION2_NAME)
        self.sync_manager.delete_action_by_name(self.ACTION1_NAME)
        self.sync_manager.delete_workflow_by_name(self.WORKFLOW1_NAME)
        self.sync_manager.delete_datastore_by_name(self.DATASTORE1_NAME)

    def synchronize(self):
        """
        Synchronize this model with Dart and ensure that all entities
        are created or updated.
        """
        ds = self.sync_manager.sync_datastore(self.DATASTORE1_NAME, self.define_datastore1)
        wf = self.sync_manager.sync_workflow(self.WORKFLOW1_NAME, ds, self.define_workflow1)
        self.sync_manager.sync_action(self.ACTION1_NAME, wf, self.define_action1)
        self.sync_manager.sync_action(self.ACTION2_NAME, wf, self.define_action2)
        self.sync_manager.sync_trigger(self.TRIGGER1_NAME, wf, self.define_trigger1)
        self.sync_manager.sync_dataset(self.DATASET1_NAME, self.define_dataset1)

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
        datastore.data.name = self.DATASTORE1_NAME
        return datastore

    def define_workflow1(self, workflow):
        workflow.data.concurrency = 1
        workflow.data.engine_name = 'emr_engine'
        workflow.data.name = self.WORKFLOW1_NAME
        return workflow

    def define_action1(self, action):
        action.data.action_type_name = 'start_datastore'
        action.data.engine_name = 'emr_engine'
        action.data.name = self.ACTION1_NAME
        action.data.order_idx = 0
        action.data.state = 'TEMPLATE'
        return action

    def define_action2(self, action):
        action.data.action_type_name = 'terminate_datastore'
        action.data.engine_name = 'emr_engine'
        action.data.name = self.ACTION2_NAME
        action.data.order_idx = 1
        action.data.state = 'TEMPLATE'
        return action

    def define_trigger1(self, trigger):
        trigger.data.args = {
            'cron_pattern': '0 0 * * ? *'
        }
        trigger.data.name = self.TRIGGER1_NAME
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
        dataset.data.name = self.DATASET1_NAME
        dataset.data.table_name = 'table1'
        dataset.data.user_id = 'user1'
        return dataset

    def check_clean(self):
        """
        Assert that the entities do not exist in Dart.
        :return:
        """
        assert self.sync_manager.find_datastore_by_name(self.DATASTORE1_NAME) is None
        assert self.sync_manager.find_workflow_by_name(self.WORKFLOW1_NAME) is None
        assert self.sync_manager.find_action_by_name(self.ACTION1_NAME) is None
        assert self.sync_manager.find_action_by_name(self.ACTION2_NAME) is None
        assert self.sync_manager.find_trigger_by_name(self.TRIGGER1_NAME) is None
        assert self.sync_manager.find_dataset_by_name(self.DATASET1_NAME) is None

    def validate(self):
        """
        Validate that the entities in Dart are what this class defined them to be.
        :return:
        """
        self.validate_datastore1()
        self.validate_workflow1()
        self.validate_action1()
        self.validate_action2()
        self.validate_trigger1()
        self.validate_dataset1()

    def validate_datastore1(self):
        datastore = self.sync_manager.find_datastore_by_name(self.DATASTORE1_NAME)
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

    def validate_workflow1(self):
        workflow = self.sync_manager.find_workflow_by_name(self.WORKFLOW1_NAME)
        self.validate_object(workflow)

        assert workflow.data.concurrency == 1
        assert workflow.data.engine_name == 'emr_engine'
        assert workflow.data.name == self.WORKFLOW1_NAME

    def validate_action1(self):
        action = self.sync_manager.find_action_by_name(self.ACTION1_NAME)
        self.validate_object(action)

        assert action.data.action_type_name == 'start_datastore'
        assert action.data.engine_name == 'emr_engine'
        assert action.data.name == self.ACTION1_NAME
        assert action.data.order_idx == 0
        assert action.data.state == 'TEMPLATE'

    def validate_action2(self):
        action = self.sync_manager.find_action_by_name(self.ACTION2_NAME)
        self.validate_object(action)

        assert action.data.action_type_name == 'terminate_datastore'
        assert action.data.engine_name == 'emr_engine'
        assert action.data.name == self.ACTION2_NAME
        assert action.data.order_idx == 1
        assert action.data.state == 'TEMPLATE'

    def validate_trigger1(self):
        trigger = self.sync_manager.find_trigger_by_name(self.TRIGGER1_NAME)
        self.validate_object(trigger)

        assert trigger.data.args == {
            'cron_pattern': '0 0 * * ? *'
        }
        assert trigger.data.name == self.TRIGGER1_NAME
        assert trigger.data.trigger_type_name == 'scheduled'

    def validate_dataset1(self):
        dataset = self.sync_manager.find_dataset_by_name(self.DATASET1_NAME)
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
        assert dataset.data.user_id == 'user1'

    def validate_object(self, obj):
        assert obj.created is not None and isinstance(obj.created, basestring)
        assert obj.data is not None
        assert obj.id is not None and isinstance(obj.id, basestring)
        assert obj.updated is not None and isinstance(obj.updated, basestring)
        assert obj.version_id is not None and isinstance(obj.version_id, int)
        if hasattr(obj.data, 'on_failure_email'):
            assert obj.data.on_failure_email == self.DEFAULTS['on_failure_email']
        if hasattr(obj.data, 'on_started_email'):
            assert obj.data.on_started_email == self.DEFAULTS['on_started_email']
        if hasattr(obj.data, 'on_success_email'):
            assert obj.data.on_success_email == self.DEFAULTS['on_success_email']
        assert obj.data.tags == self.DEFAULTS['tags']
