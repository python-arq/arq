#!/usr/bin/env python3
import os
import sys

from arq.version import VERSION

git_tag = os.getenv('TRAVIS_TAG')
if git_tag:
    if git_tag != str(VERSION):
        print('✖ "TRAVIS_TAG" environment variable does not match arq.version: "%s" vs. "%s"' % (git_tag, VERSION))
        sys.exit(1)
    else:
        print('✓ "TRAVIS_TAG" environment variable matches arq.version: "{}"'.format(VERSION))
else:
    print('✓ "TRAVIS_TAG" not defined')
