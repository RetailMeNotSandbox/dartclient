# The MIT License (MIT)
#
# Copyright (c) 2016 RetailMeNot, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from bravado.client import SwaggerClient

import pkg_resources
import yaml


def create_client(origin_url=None, config=None, api_url=None):
    """
    Create the Bravado swagger client from the specified origin url and config.
    For the moment, the Swagger specification for Dart is actually bundled
    with this client since Dart does not have an endpoint that exposes it
    dynamically.

    :param origin_url:
    :param config:
    :return:
    """
    # Load the swagger from the resource file
    with pkg_resources.resource_stream('dartclient', 'swagger.yaml') as f:
        spec_dict = yaml.load(f)
    client = SwaggerClient.from_spec(spec_dict,
                                     origin_url=origin_url,
                                     config=config)
    if api_url:
        client.swagger_spec.api_url = api_url
    return client


def create_sync_manager(client=None,
                        origin_url=None,
                        api_url=None,
                        config=None,
                        model_factory=None,
                        model_defaults=None):
    """
    Convenient method to create a SyncManager instance.
    :param client: bravado.client.SwaggerClient instance
    :param origin_url: Origin URL for constructing a SwaggerClient from a
        Swagger specification if one is not supplied
    :param api_url: the Dart API URL
    :param config: Configuration dictionary for constructing a SwaggerClient
         if one is not supplied
    :param model_factory: ModelFactory instance
    :param model_defaults: Dictionary of default values for construction
        a ModelFactory if one is not supplied
    :return:
    """
    client = client or create_client(origin_url=origin_url, config=config, api_url=api_url)
    model_factory = model_factory or ModelFactory(client, **(model_defaults or {}))
    return SyncManager(client, model_factory)


class ModelFactory(object):
    """
    Provides factory methods for model objects that are used by SyncManager.
    You can sub-class to set project level defaults for each of the various
    model types.
    """
    def __init__(self,
                 client,
                 on_failure_email=None,
                 on_started_email=None,
                 on_success_email=None,
                 tags=None):
        self.client = client
        self.on_failure_email = on_failure_email or []
        self.on_started_email = on_started_email or []
        self.on_success_email = on_success_email or []
        self.tags = tags or []

    def create_datastore(self):
        """
        Create a datastore object and set the tags field to the default.
        :return: the datastore object
        """
        get_model = self.client.get_model
        datastore = get_model('Datastore')(data=get_model('DatastoreData')())
        datastore.data.tags = self.tags
        return datastore

    def create_workflow(self):
        """
        Create a workflow object and set the on_failure_email, on_started_email,
        on_success_email, and tags fields to the defaults.
        :return: the workflow object
        """
        get_model = self.client.get_model
        workflow = get_model('Workflow')(data=get_model('WorkflowData')())
        workflow.data.on_failure_email = self.on_failure_email
        workflow.data.on_started_email = self.on_started_email
        workflow.data.on_success_email = self.on_success_email
        workflow.data.tags = self.tags
        return workflow

    def create_action(self):
        """
        Construct an action object and set the on_failure_email,
        on_success_email, and tags fields to the defaults.
        :return: the action object
        """
        get_model = self.client.get_model
        action = get_model('Action')(data=get_model('ActionData')())
        action.data.on_failure_email = self.on_failure_email
        action.data.on_success_email = self.on_success_email
        action.data.tags = self.tags
        return action

    def create_trigger(self):
        """
        Construct a trigger object and set the tags field to the default.
        :return: the trigger object
        """
        get_model = self.client.get_model
        trigger = get_model('Trigger')(data=get_model('TriggerData')())
        trigger.data.tags = self.tags
        return trigger

    def create_dataset(self):
        """
        Construct a dataset object and set the tags field to the default.
        :return: the dataset object
        """
        get_model = self.client.get_model
        dataset = get_model('Dataset')(data=get_model('DatasetData')())
        dataset.data.data_format = get_model('DataFormat')()
        dataset.data.tags = self.tags
        return dataset


class SyncManager(object):
    """
    Provides convenient methods for synchronizing descriptions of a Dart
    model with a Dart server.
    """

    def __init__(self, client, model_factory):
        self.client = client
        self.model_factory = model_factory

    def filter_by_name(self, name):
        """
        Convenience method to create a filter expression to find objects by name
        :param name: the object name
        :return: the filter expression
        """
        return '["name = %s"]' % (name,)

    def find_datastore_by_name(self, datastore_name):
        """
        Find the datastore by name
        :param datastore_name: the datastore name
        :return: the datastore object or None if not found
        """
        response = self.client.Datastore.listDatastores(filters=self.filter_by_name(datastore_name)).result()
        return response.results[0] if response.total > 0 else None

    def find_workflow_by_name(self, workflow_name):
        """
        Find the workflow by name
        :param workflow_name: the workflow name
        :return: the workflow object or None if not found
        """
        response = self.client.Workflow.listWorkflows(filters=self.filter_by_name(workflow_name)).result()
        return response.results[0] if response.total > 0 else None

    def find_action_by_name(self, action_name):
        """
        Find the action by name
        :param action_name: the action name
        :return: the action object or None if not found
        """
        response = self.client.Action.listActions(filters=self.filter_by_name(action_name)).result()
        return response.results[0] if response.total > 0 else None

    def find_trigger_by_name(self, trigger_name):
        """
        Find the trigger by name
        :param trigger_name: the trigger name
        :return: the trigger object or None if not found
        """
        response = self.client.Trigger.listTriggers(filters=self.filter_by_name(trigger_name)).result()
        return response.results[0] if response.total > 0 else None

    def find_dataset_by_name(self, dataset_name):
        """
        Find the dataset by name
        :param dataset_name: the dataset name
        :return: the dataset object or None if not found
        """
        response = self.client.Dataset.listDatasets(filters=self.filter_by_name(dataset_name)).result()
        return response.results[0] if response.total > 0 else None

    def delete_datastore_by_name(self, datastore_name):
        """
        Find the datastore by name and delete it if found.
        :param datastore_name: the datastore name
        :return: the datastore object or None if not found
        """
        datastore = self.find_datastore_by_name(datastore_name)
        if datastore:
            self.client.Datastore.deleteDatastore(datastore_id=datastore.id).result()

    def delete_workflow_by_name(self, workflow_name):
        """
        Find the workflow by name and delete it if found.
        :param workflow_name: the workflow name
        :return:
        """
        workflow = self.find_workflow_by_name(workflow_name)
        if workflow:
            self.client.Workflow.deleteWorkflow(workflow_id=workflow.id).result()

    def delete_action_by_name(self, action_name):
        """
        Find the action by name and delete it if found.
        :param action_name: the action name
        :return:
        """
        action = self.find_action_by_name(action_name)
        if action:
            self.client.Action.deleteAction(action_id=action.id).result()

    def delete_trigger_by_name(self, trigger_name):
        """
        Find the trigger by name and delete it if found.
        :param trigger_name: the trigger name
        :return:
        """
        trigger = self.find_trigger_by_name(trigger_name)
        if trigger:
            self.client.Trigger.deleteTrigger(trigger_id=trigger.id).result()

    def delete_dataset_by_name(self, dataset_name):
        """
        Find the dataset by name and delete it if found.
        :param dataset_name:
        :return:
        """
        dataset = self.find_dataset_by_name(dataset_name)
        if dataset:
            self.client.Dataset.deleteDataset(dataset_id=dataset.id).result()

    def sync_datastore(self, datastore_name, callback):
        """
        Synchronize a datastore with Dart.

        :param datastore_name: The name of the datastore.
        :param callback: A function with a signature (datastore) => datastore
        :return: The created or updated datastore.
        """
        datastore = self.find_datastore_by_name(datastore_name)
        if datastore:
            datastore = callback(datastore)
            response = self.client.Datastore.updateDatastore(datastore_id=datastore.id, datastore=datastore).result()
            return response.results
        else:
            datastore = callback(self.model_factory.create_datastore())
            response = self.client.Datastore.createDatastore(datastore=datastore).result()
            return response.results

    def sync_workflow(self, workflow_name, datastore, callback):
        """
        Synchronize a workflow with Dart.

        :param workflow_name: The name of the workflow.
        :param datastore: The datastore containing the workflow.
        :param callback: A function with a signature (workflow) => workflow
        :return: The created or updated workflow.
        """
        workflow = self.find_workflow_by_name(workflow_name)
        if workflow:
            workflow = callback(workflow)
            response = self.client.Workflow.updateWorkflow(workflow_id=workflow.id, workflow=workflow).result()
            return response.results
        else:
            workflow = callback(self.model_factory.create_workflow())
            workflow.data.datastore_id = datastore.id
            response = self.client.Datastore.createDatastoreWorkflow(datastore_id=datastore.id, workflow=workflow).result()
            return response.results

    def sync_action(self, action_name, workflow, callback):
        """
        Synchronize an action with Dart.

        :param action_name: The name of the action.
        :param workflow: The workflow containing the action.
        :param callback: A function with a signature (action) => action
        :return: The created or updated action.
        """
        action = self.find_action_by_name(action_name)
        if action:
            action = callback(action)
            response = self.client.Action.updateAction(action_id=action.id, action=action).result()
            return response.results
        else:
            action = callback(self.model_factory.create_action())
            response = self.client.Workflow.createWorkflowActions(workflow_id=workflow.id, actions=[action]).result()
            return response.results[0]

    def sync_trigger(self, trigger_name, workflow, callback):
        """
        Synchronize a trigger with Dart.

        :param trigger_name: The name of the trigger
        :param callback: A function with a signature (trigger) => trigger
        :return: The created or updated trigger.
        """
        trigger = self.find_trigger_by_name(trigger_name)
        if trigger:
            trigger = callback(trigger)
            response = self.client.Trigger.updateTrigger(trigger_id=trigger.id, trigger=trigger).result()
            return response.results
        else:
            trigger = callback(self.model_factory.create_trigger())
            if workflow:
                trigger.data.workflow_ids = [workflow.id]
            response = self.client.Trigger.createTrigger(trigger=trigger).result()
            return response.results

    def sync_dataset(self, dataset_name, callback):
        """
        Synchronize a dataset with Dataset.

        :param dataset_name: The name of the dataset
        :param callback: A function with a signature (dataset) => dataset
        :return: The created or updated dataset
        """
        dataset = self.find_dataset_by_name(dataset_name)
        if dataset:
            dataset = callback(dataset)
            response = self.client.Dataset.updateDataset(dataset_id=dataset.id, dataset=dataset).result()
            return response.results
        else:
            dataset = callback(self.model_factory.create_dataset())
            response = self.client.Dataset.createDataset(dataset=dataset).result()
            return response.results
