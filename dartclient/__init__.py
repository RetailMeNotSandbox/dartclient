from bravado.client import SwaggerClient

import pkg_resources
import yaml

###
# import requests
# import logging
#
# try:
#     import httplib
# except ImportError:
#     import http.client as httplib
#
# httplib.HTTPConnection.debuglevel = 1
#
# logging.basicConfig()
# logging.getLogger().setLevel(logging.DEBUG)
# requests_log = logging.getLogger("requests.packages.urllib3")
# requests_log.setLevel(logging.DEBUG)
# requests_log.propagate = True
###


def create_client(origin_url, config):
    # Load the swagger from the resource file
    with pkg_resources.resource_stream('dartclient', 'swagger.yaml') as f:
        spec_dict = yaml.load(f)
    return SwaggerClient.from_spec(spec_dict, origin_url=origin_url, config=config)


# Create the default client
_client = create_client(
    origin_url=None,
    config={
        #    'validate_swagger_spec': False,
        #    'validate_requests': False,
        #    'validate_responses': False,
        #    'use_models': False
    })

# import models into sdk package
Action = _client.get_model('Action')
ActionContext = _client.get_model('ActionContext')
ActionContextResponse = _client.get_model('ActionContextResponse')
ActionData = _client.get_model('ActionData')
ActionResponse = _client.get_model('ActionResponse')
ActionsResponse = _client.get_model('ActionsResponse')
ActionResult = _client.get_model('ActionResult')
ActionType = _client.get_model('ActionType')
Column = _client.get_model('Column')
DataFormat = _client.get_model('DataFormat')
Dataset = _client.get_model('Dataset')
DatasetData = _client.get_model('DatasetData')
DatasetResponse = _client.get_model('DatasetResponse')
Datastore = _client.get_model('Datastore')
DatastoreData = _client.get_model('DatastoreData')
DatastoreResponse = _client.get_model('DatastoreResponse')
Engine = _client.get_model('Engine')
EngineData = _client.get_model('EngineData')
EngineResponse = _client.get_model('EngineResponse')
ErrorResponse = _client.get_model('ErrorResponse')
Event = _client.get_model('Event')
EventData = _client.get_model('EventData')
EventResponse = _client.get_model('EventResponse')
Filter = _client.get_model('Filter')
GraphEntity = _client.get_model('GraphEntity')
GraphEntityIdentifier = _client.get_model('GraphEntityIdentifier')
GraphEntityIdentifierResponse = _client.get_model('GraphEntityIdentifierResponse')
GraphEntityIdentifiersResponse = _client.get_model('GraphEntityIdentifiersResponse')
GraphEntityResponse = _client.get_model('GraphEntityResponse')
JSONPatch = _client.get_model('JSONPatch')
JSONSchema = _client.get_model('JSONSchema')
JSONSchemaResponse = _client.get_model('JSONSchemaResponse')
OKResponse = _client.get_model('OKResponse')
ObjectResponse = _client.get_model('ObjectResponse')
ObjectsResponse = _client.get_model('ObjectsResponse')
OrderBy = _client.get_model('OrderBy')
PagedActionsResponse = _client.get_model('PagedActionsResponse')
PagedDatasetsResponse = _client.get_model('PagedDatasetsResponse')
PagedDatastoresResponse = _client.get_model('PagedDatastoresResponse')
PagedEnginesResponse = _client.get_model('PagedEnginesResponse')
PagedEventsResponse = _client.get_model('PagedEventsResponse')
PagedObjectsResponse = _client.get_model('PagedObjectsResponse')
PagedSubscriptionElementsResponse = _client.get_model('PagedSubscriptionElementsResponse')
PagedSubscriptionsResponse = _client.get_model('PagedSubscriptionsResponse')
PagedTriggerTypesResponse = _client.get_model('PagedTriggerTypesResponse')
PagedTriggersResponse = _client.get_model('PagedTriggersResponse')
PagedWorkflowInstancesResponse = _client.get_model('PagedWorkflowInstancesResponse')
PagedWorkflowsResponse = _client.get_model('PagedWorkflowsResponse')
Subgraph = _client.get_model('Subgraph')
SubgraphDefinition = _client.get_model('SubgraphDefinition')
SubgraphDefinitionResponse = _client.get_model('SubgraphDefinitionResponse')
SubgraphResponse = _client.get_model('SubgraphResponse')
Subscription = _client.get_model('Subscription')
SubscriptionData = _client.get_model('SubscriptionData')
SubscriptionElement = _client.get_model('SubscriptionElement')
SubscriptionResponse = _client.get_model('SubscriptionResponse')
Trigger = _client.get_model('Trigger')
TriggerData = _client.get_model('TriggerData')
TriggerResponse = _client.get_model('TriggerResponse')
TriggerType = _client.get_model('TriggerType')
Workflow = _client.get_model('Workflow')
WorkflowData = _client.get_model('WorkflowData')
WorkflowInstance = _client.get_model('WorkflowInstance')
WorkflowInstanceData = _client.get_model('WorkflowInstanceData')
WorkflowInstanceResponse = _client.get_model('WorkflowInstanceResponse')
WorkflowResponse = _client.get_model('WorkflowResponse')


class ModelFactory(object):
    """
    Provides factory methods for model objects that are used by SyncManager.
    You can sub-class to set project level defaults for each of the various
    model types.
    """
    def __init__(self,
                 on_failure_email=None,
                 on_started_email=None,
                 on_success_email=None,
                 tags=None):
        self.on_failure_email = on_failure_email or []
        self.on_started_email = on_started_email or []
        self.on_success_email = on_success_email or []
        self.tags = tags or []

    def create_datastore(self):
        datastore = Datastore(data=DatastoreData())
        datastore.data.tags = self.tags
        return datastore

    def create_workflow(self):
        workflow = Workflow(data=WorkflowData())
        workflow.data.on_failure_email = self.on_failure_email
        workflow.data.on_stated_email = self.on_started_email
        workflow.data.on_success_email = self.on_success_email
        workflow.data.tags = self.tags
        return workflow

    def create_action(self):
        action = Action(data=ActionData())
        action.data.on_failure_email = self.on_failure_email
        action.data.on_success_email = self.on_success_email
        action.data.tags = self.tags
        return action


class SyncManager(object):
    """
    Provides convenient methods for synchronizing descriptions of the Dart workflow with the Dart server.
    """

    def __init__(self, client, model_factory):
        self.client = client
        self.model_factory = model_factory
        self.datastore_api = client.Datastore
        self.workflow_api = client.Workflow
        self.action_api = client.Action

    def sync_datastore(self, datastore_name, callback):
        """
        Synchronize a datastore with Dart.

        :param datastore_name: The name of the datastore.
        :param callback: A function with a signature (datastore) => datastore
        :return: The created or updated datastore.
        """
        response = self.datastore_api.listDatastores(filters='["name = %s"]' % (datastore_name,), limit=20, offset=0).result()
        if response.total > 0:
            datastore = callback(response.results[0])
            response = self.datastore_api.updateDatastore(datastore_id=datastore.id, datastore=datastore).result()
            return response.results
        else:
            datastore = callback(self.model_factory.create_datastore())
            response = self.datastore_api.createDatastore(datastore=datastore).result()
            return response.results

    def sync_workflow(self, workflow_name, datastore, callback):
        """
        Synchronize a workflow with Dart.

        :param workflow_name: The name of the workflow.
        :param datastore: The datastore containing the workflow.
        :param callback: A function with a signature (workflow) => workflow
        :return: The created or updated workflow.
        """
        response = self.workflow_api.listWorkflows(filters='["name = %s"]' % (workflow_name,), limit=20, offset=0).result()
        if response.total > 0:
            workflow = callback(response.results[0])
            response = self.workflow_api.updateWorkflow(workflow_id=workflow.id, workflow=workflow).result()
            return response.results
        else:
            workflow = callback(self.model_factory.create_workflow())
            workflow.data.datastore_id = datastore.id
            response = self.datastore_api.createDatastoreWorkflow(datastore_id=datastore.id, workflow=workflow).result()
            return response.results

    def sync_action(self, action_name, workflow, callback):
        response = self.action_api.listActions(filters='["name = %s"]' % (action_name,), limit=20, offset=0).result()
        if response.total > 0:
            action = callback(response.results[0])
            response = self.action_api.updateAction(action_id=action.id, action=action).result()
            return response.results
        else:
            action = callback(Action(self.model_factory.create_action()))
            response = self.workflow_api.createWorkflowActions(workflow_id=workflow.id, actions=[action]).result()
            return response.results[0]


def create_sync_manager(client=None, model_factory=None, api_url=None):
    client = client or _client
    model_factory = model_factory or ModelFactory()
    if api_url:
        client.swagger_spec.api_url = api_url
    return SyncManager(client, model_factory)
