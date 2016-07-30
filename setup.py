from importlib.machinery import SourceFileLoader
from setuptools import setup

long_description = """
|Build Status| |Coverage|

Job queues in python with asyncio, redis and msgpack.

rq meets asyncio.

See `github <https://github.com/samuelcolvin/arq>`__ for more details.

.. |Build Status| image:: https://travis-ci.org/samuelcolvin/arq.svg?branch=master
   :target: https://travis-ci.org/samuelcolvin/arq
.. |Coverage| image:: https://codecov.io/github/samuelcolvin/arq/coverage.svg?branch=master
   :target: https://codecov.io/github/samuelcolvin/arq?branch=master
"""

# avoid loading the package before requirements are installed:
version = SourceFileLoader('version', 'arq/version.py').load_module()

setup(
    name='arq',
    version=str(version.VERSION),
    description='Job queues in python with asyncio, redis and msgpack.',
    long_description=long_description,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Science/Research',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Operating System :: Unix',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Internet',
        'Topic :: System :: Distributed Computing',
        'Topic :: System :: Systems Administration',
        'Topic :: System :: Monitoring',
    ],
    author='Samuel Colvin',
    author_email='s@muelcolvin.com',
    url='https://github.com/samuelcolvin/arq',
    license='MIT',
    packages=['arq'],
    zip_safe=True,
    entry_points="""
        [console_scripts]
        arq=arq.cli:cli
    """,
    install_requires=[
        'aioredis>=0.2.7',
        'click>=6.6',
        'msgpack-python>=0.4.7',
    ],
)
