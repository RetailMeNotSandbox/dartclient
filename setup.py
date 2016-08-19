from setuptools import setup

import os
from dartclient import __version__

setup(
    name='dartclient',
    version=__version__,
    author='RetailMeNot, Inc.',
    url='https://github.com/RetailMeNotSandbox/dartclient',
    download_url='https://github.com/RetailMeNotSandbox/dartclient/tarball/%s'
                 % (__version__,),
    description='Client library for interacting with Dart via its API.',
    long_description=open(os.path.join(os.path.dirname(__file__), 'README.rst'), 'r').read(),
    license='MIT',
    classifiers=(
        'Development Status :: 3 - Alpha',
        'Environment :: Other Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ),
    packages=['dartclient'],
    package_data={
        '': ['*.yaml']
    },
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'bravado>=8.3.0',
        'bravado_core>=4.3.2'
    ]
)
