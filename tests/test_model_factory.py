

def test_create_datastore(model_factory, model_defaults):
    datastore = model_factory.create_datastore()
    assert_common_properties(datastore, model_defaults)


def test_create_workflow(model_factory, model_defaults):
    workflow = model_factory.create_workflow()
    assert_common_properties(workflow, model_defaults)
    assert workflow.data.on_failure_email == model_defaults.get('on_failure_email')
    assert workflow.data.on_started_email == model_defaults.get('on_started_email')
    assert workflow.data.on_success_email == model_defaults.get('on_success_email')


def test_create_action(model_factory, model_defaults):
    action = model_factory.create_action()
    assert_common_properties(action, model_defaults)
    assert action.data.on_failure_email == model_defaults.get('on_failure_email')
    assert action.data.on_success_email == model_defaults.get('on_success_email')


def test_create_trigger(model_factory, model_defaults):
    trigger = model_factory.create_trigger()
    assert_common_properties(trigger, model_defaults)


def test_create_dataset(model_factory, model_defaults):
    dataset = model_factory.create_dataset()
    assert_common_properties(dataset, model_defaults)
    assert dataset.data.data_format is not None


def assert_common_properties(obj, model_defaults):
    assert obj.created is None
    assert obj.data is not None
    assert obj.id is None
    assert obj.updated is None
    assert obj.version_id is None
    assert obj.data.tags == model_defaults['tags']
