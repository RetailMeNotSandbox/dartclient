dartclient
==========

This is a client for Dart built from its Swagger specification. It provides
classes to help you manage and synchronize your Dart model from source code
to the Dart server.

Installation
------------

Dartclient has not yet been uploaded to pypi and depends on unreleases fixes
to upstream libraries. For now, you will need to install it from source.

* git clone this repository
* pip install -r requirements-dev.txt

Related Projects
----------------
* `bravado <https://github.com/Yelp/bravado>`__

Development
===========

| `virtualenv <http://virtualenv.readthedocs.org/en/latest/virtualenv.html>`__
| `tox <https://tox.readthedocs.org/en/latest/>`__

Setup
-----

::

    # Install tox
    pip install tox

    # Run tests
    tox

    # Install git pre-commit hooks
    .tox/py27/bin/pre-commit install


License
-------

| Copyright (c) 2016, RetailMeNot, Inc. All rights reserved.

dartclient is licensed using the `MIT License <https://opensource.org/licenses/MIT>`__.
