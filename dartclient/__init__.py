from bravado.client import SwaggerClient, CONFIG_DEFAULTS
from bravado.requests_client import RequestsClient
from bravado_core.spec import Spec

import pkg_resources
import yaml

# Load the swagger from the resource file
with pkg_resources.resource_stream('dartclient', 'swagger.yaml') as f:
    spec_dict = yaml.load(f)

# Construct the Spec by hand to gain access to the api_url property
swagger_spec = Spec.from_dict(
    spec_dict,
    origin_url=None,
    http_client=RequestsClient(),
    config=dict(CONFIG_DEFAULTS, **({})))

# Create the client so that we can get to the models and APIs.
# It's annoying that the creators of Bravado decided to do things
# in this way, since it makes it difficult to build a module that
# mimics the output of swagger-codegen
client = SwaggerClient(swagger_spec)

# import models into sdk package
Action = client.get_model('Action')
ActionContext = client.get_model('ActionContext')
ActionContextResponse = client.get_model('ActionContextResponse')
ActionData = client.get_model('ActionData')
ActionResponse = client.get_model('ActionResponse')
ActionResult = client.get_model('ActionResult')
ActionType = client.get_model('ActionType')
Column = client.get_model('Column')
DataFormat = client.get_model('DataFormat')
Dataset = client.get_model('Dataset')
DatasetData = client.get_model('DatasetData')
DatasetResponse = client.get_model('DatasetResponse')
Datastore = client.get_model('Datastore')
DatastoreData = client.get_model('DatastoreData')
DatastoreResponse = client.get_model('DatastoreResponse')
Engine = client.get_model('Engine')
EngineData = client.get_model('EngineData')
EngineResponse = client.get_model('EngineResponse')
ErrorResponse = client.get_model('ErrorResponse')
Event = client.get_model('Event')
EventData = client.get_model('EventData')
EventResponse = client.get_model('EventResponse')
Filter = client.get_model('Filter')
GraphEntity = client.get_model('GraphEntity')
GraphEntityIdentifier = client.get_model('GraphEntityIdentifier')
GraphEntityIdentifierResponse = client.get_model('GraphEntityIdentifierResponse')
GraphEntityIdentifiersResponse = client.get_model('GraphEntityIdentifiersResponse')
GraphEntityResponse = client.get_model('GraphEntityResponse')
JSONPatch = client.get_model('JSONPatch')
JSONSchema = client.get_model('JSONSchema')
JSONSchemaResponse = client.get_model('JSONSchemaResponse')
OKResponse = client.get_model('OKResponse')
ObjectResponse = client.get_model('ObjectResponse')
ObjectsResponse = client.get_model('ObjectsResponse')
OrderBy = client.get_model('OrderBy')
PagedActionsResponse = client.get_model('PagedActionsResponse')
PagedDatasetsResponse = client.get_model('PagedDatasetsResponse')
PagedDatastoresResponse = client.get_model('PagedDatastoresResponse')
PagedEnginesResponse = client.get_model('PagedEnginesResponse')
PagedEventsResponse = client.get_model('PagedEventsResponse')
PagedObjectsResponse = client.get_model('PagedObjectsResponse')
PagedSubscriptionElementsResponse = client.get_model('PagedSubscriptionElementsResponse')
PagedSubscriptionsResponse = client.get_model('PagedSubscriptionsResponse')
PagedTriggerTypesResponse = client.get_model('PagedTriggerTypesResponse')
PagedTriggersResponse = client.get_model('PagedTriggersResponse')
PagedWorkflowInstancesResponse = client.get_model('PagedWorkflowInstancesResponse')
PagedWorkflowsResponse = client.get_model('PagedWorkflowsResponse')
Subgraph = client.get_model('Subgraph')
SubgraphDefinition = client.get_model('SubgraphDefinition')
SubgraphDefinitionResponse = client.get_model('SubgraphDefinitionResponse')
SubgraphResponse = client.get_model('SubgraphResponse')
Subscription = client.get_model('Subscription')
SubscriptionData = client.get_model('SubscriptionData')
SubscriptionElement = client.get_model('SubscriptionElement')
SubscriptionResponse = client.get_model('SubscriptionResponse')
Trigger = client.get_model('Trigger')
TriggerData = client.get_model('TriggerData')
TriggerResponse = client.get_model('TriggerResponse')
TriggerType = client.get_model('TriggerType')
Workflow = client.get_model('Workflow')
WorkflowData = client.get_model('WorkflowData')
WorkflowInstance = client.get_model('WorkflowInstance')
WorkflowInstanceData = client.get_model('WorkflowInstanceData')
WorkflowInstanceResponse = client.get_model('WorkflowInstanceResponse')
WorkflowResponse = client.get_model('WorkflowResponse')

# import apis into sdk package
ActionApi = client.Action
DatasetApi = client.Dataset
DatastoreApi = client.Datastore
EngineApi = client.Engine
EventApi = client.Event
GraphApi = client.Graph
GraphEntityIdentifierApi = client.GraphEntityIdentifier
SchemaApi = client.Schema
SubgraphApi = client.Subgraph
SubgraphDefinitionApi = client.SubgraphDefinition
SubscriptionApi = client.Subscription
TriggerApi = client.Trigger
TriggerTypeApi = client.TriggerType
WorkflowApi = client.Workflow
WorkflowInstanceApi = client.WorkflowInstance
