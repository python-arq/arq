# arq demo with docker compose and aiohttp

The directory contains files required to serve the demo docker/aiohttp example.

## Usage

**(all from the project root directory)**

To build:

    ./demo/build.sh

To run the compose example:

    export COMPOSE_FILE='demo/docker-compose.yml'
    export COMPOSE_PROJECT_NAME='arq'
    docker-compose up -d

You'll want to then connect to logspout to view the log with something like

    curl -q -s http://localhost:5001/logs
