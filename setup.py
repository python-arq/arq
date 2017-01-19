from pathlib import Path

from importlib.machinery import SourceFileLoader
from setuptools import setup

with Path(__file__).resolve().parent.joinpath('README.rst').open() as f:
    long_description = f.read()

# avoid loading the package before requirements are installed:
version = SourceFileLoader('version', 'arq/version.py').load_module()

setup(
    name='arq',
    version=str(version.VERSION),
    description='Job queues in python with asyncio, redis and msgpack.',
    long_description=long_description,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Operating System :: Unix',
        'Operating System :: POSIX :: Linux',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Internet',
        'Topic :: System :: Clustering',
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
        'aioredis>=0.2.9',
        'click>=6.6',
        'msgpack-python>=0.4.8',
    ],
    extras_require={
        'testing': ['pytest>=3.0.5'],
    },
)
