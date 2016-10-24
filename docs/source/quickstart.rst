.. _quickstart:

Quickstart
==========

Usage
-----

Dartclient has not yet been uploaded to pypi. For now, you will need to
install it from source:

* pip install git+https://github.com/RetailMeNotSandbox/dartclient.git

Connecting to Dart
------------------

Here is a simple example to make your first connection to Dart. Note that in
a real application the host and credentials should not be embedded into the
source.

.. code-block:: python

    from dartclient.core import create_client, create_basic_authenticator
    import json

    # Setting the host and credentials for this example only
    host = "your-dart-server"
    username = "youruser"
    password = "yourpassword"

    authenticator = create_basic_authenticator(
        host,
        username=username,
        password=password)
    client = create_client(
        api_url='https://%s/api/1' % (host,),
        authenticator=authenticator)

    response = client.Datastore.listDatastores().result()
    print(json.dumps(response, indent=True))

Managing Your Model with SyncManager
------------------------------------

Dartclient provides a class called SyncManager that is intended to help you
manage your model in Dart. This example adds construction and usage of the
SyncManager to our earlier example:

.. code-block:: python

    from dartclient.core import create_client, \
        create_basic_authenticator, \
        create_sync_manager
    import json

    # Setting the host and credentials for this example only
    host = "your-dart-server"
    username = "youruser"
    password = "yourpassword"

    authenticator = create_basic_authenticator(
        host,
        username=username,
        password=password)
    client = create_client(
        api_url='https://%s/api/1' % (host,),
        authenticator=authenticator)
    sync_manager = create_sync_manager(client=client)

    datastore = sync_manager.find_datastore(
        datastore_name='My Datastore',
        datastore_state='ACTIVE')
    print(datastore)

SyncManager provides a set of methods to find, sync, and clean various model
objects in Dart include datastores, datasets, workflows, etc. It is recommended
that you organize your model management code into a class similar to the following
example:

.. code-block:: python

    class MyAppModel(object):

        DEFAULTS = {
            'on_failure_email': ['myuser@example.com'],
            'tags': ['myapp']
        }

        def __init__(self, client):
            self.client = client
            self.sync_manager = create_sync_manager(
                client=client,
                model_defaults=self.DEFAULTS)

        def clean(self):
            """
            Remove this model from Dart
            """
            self.sync_manager.clean_datastore(
                self.sync_manager.find_datastore('myapp_emr_cluster'))

        def synchronize(self):
            """
            Create or update this model in Dart
            """
            ds = self.sync_manager.sync_datastore(
                'myapp_emr_cluster', 'TEMPLATE',
                self.define_emr_cluster)

            wf = self.sync_manager.sync_workflow(
                'myapp_workflow', ds,
                self.define_workflow)

            self.sync_manager.sync_action(
                'myapp_start_emr_cluster', wf,
                self.define_start_emr_cluster_action)

            self.sync_manager.sync_action(
                'myapp_pyspark_script', wf,
                self.define_pyspark_script)

            self.sync_manager.sync_action(
                'myapp_terminate_emr_cluster', wf,
                self.define_terminate_emr_cluster_action)

        def define_emr_cluster(self, datastore):
            datastore.data.args = {
                "data_to_freespace_ratio": 0.5,
                "dry_run": False,
                "instance_count": 3,
                "instance_type": "m3.2xlarge",
                "release_label": "emr-4.3.0"
            }
            datastore.data.concurrency = 1
            datastore.data.engine_name = 'emr_engine'
            return datastore

        def define_workflow(self, workflow):
            workflow.data.concurrency = 1
            workflow.data.engine_name = 'emr_engine'
            return workflow

        def define_start_emr_cluster_action(self, action):
            action.data.action_type_name = 'start_datastore'
            action.data.engine_name = 'emr_engine'
            action.data.order_idx = 0
            action.data.state = 'TEMPLATE'
            return action

        def define_pyspark_script(self, action):
            action.data.action_type_name = 'run_pyspark_script'
            action.data.args = {
                'script_contents':
                    pkg_resources.resource_string(__name__, 'transform.py')
            }
            action.data.engine_name = 'emr_engine'
            action.data.order_idx = 1
            action.data.state = 'TEMPLATE'
            return action

        def define_terminate_emr_cluster_action(self, action):
            action.data.action_type_name = 'terminate_datastore'
            action.data.engine_name = 'emr_engine'
            action.data.order_idx = 2
            action.data.state = 'TEMPLATE'
            return action


Creating a CLI for Your Model
-----------------------------

Once you have a model class, you'll need some way to execute it. The click
package makes this super easy.

.. code-block:: python

    import click
    import os

    from dartclient.core import create_client, create_basic_authenticator
    from myapp.dart import MyAppModel


    @click.group(invoke_without_command=True, chain=True)
    @click.pass_context
    @click.option('--host',
                  help='Dart host name')
    @click.option('--api-key',
                  prompt=True,
                  envvar='DART_API_KEY',
                  help='Dart API key')
    @click.option('--secret-key',
                  prompt=True, hide_input=True,
                  envvar='DART_SECRET_KEY',
                  help='Dart secret key')
    def main(click_context, host, api_key, secret_key):
        authenticator = create_basic_authenticator(
            host,
            username=api_key,
            password=secret_key)
        client = create_client(
            api_url='https://%s/api/1' % (host,),
            authenticator=authenticator)
        click_context.obj = MyAppModel(client)


    @main.command()
    @click.pass_context
    def clean(click_context):
        click_context.obj.clean()


    @main.command()
    @click.pass_context
    def synchronize(click_context):
        click_context.obj.synchronize()


    if __name__ == '__main__':
        main()

Packaging it Up
---------------

Assuming you have organized the source above into:

- myapp/

 - dart.py - your model class

 - cli.py  - your CLI code

Then you can create a setup.py in the root to pack it all up and to create an executable for your CLI:

.. code-block:: python

    from setuptools import setup, find_packages

    setup(
        name='myapp',
        version='1.0.dev',
        packages=['myapp'],
        zip_safe=False,
        include_package_data=True,
        install_requires=[
            'dartclient',
            'click'
        ],
        entry_points={
            'console_scripts': [
                'myapp=myapp.cli:main'
            ]
        }
    )

Along with a requirements.txt file:

.. code-block:: text

    -e git+https://github.com/RetailMeNotSandbox/dartclient.git
    click
