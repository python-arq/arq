import sys

sys.stderr.write(
    """
===============================
Unsupported installation method
===============================
arq no longer supports installation with `python setup.py install`.
Please use `python -m pip install .` instead.
"""
)
sys.exit(1)


# The below code will never execute, however GitHub is particularly
# picky about where it finds Python packaging metadata.
# See: https://github.com/github/feedback/discussions/6456
#
# To be removed once GitHub catches up.

setup(
    name='arq',
    install_requires=[
        'redis[hiredis]>=4.2.0',
        'click>=8.0',
        'typing-extensions>=4.1.0',
    ],
)
