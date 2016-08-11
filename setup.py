from setuptools import setup

setup(
    name='dartclient',
    version='1.0.dev',
    author='Zachary Roadhouse',
    author_email='zroadhouse@rmn.com',
    description='Swagger client for Dart',
    packages=['dartclient'],
    zip_safe=False,
    include_package_data=True,
    install_requires=[
        'bravado'
    ]
)
