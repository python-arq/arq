#!/usr/bin/env bash

set -e

THIS_DIR=`dirname "$0"`

cd ${THIS_DIR}
if [[ ! -d tmp ]]; then
    echo "creating tmp directory..."
    mkdir tmp
else
    echo "tmp directory already exists"
fi

echo "copying necessary files into place..."
rsync -i -a requirements.txt tmp/docker-requirements.txt
rsync -i -a --delete --exclude=*.pyc --exclude=__pycache__ ../arq tmp/
rsync -i -a ../setup.py tmp/
rsync -i -a app.py tmp/
rsync -i -a Dockerfile tmp/

echo "building docker image..."
docker build tmp -t arq-demo
echo "done."

