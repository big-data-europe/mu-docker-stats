#!/bin/bash

DRC=$1

echo "DockerCompose: ${DRC}"

network="${DRC}_default"
echo "Network ${network}"

link="${DRC}_database_1"
echo -en "Link: ${link}\n"

docker run -it --rm \
    --network "${network}" \
    -p 1234:80 -v "$PWD":/src \
    --link database:"${link}" \
    --name mu-docker-stats  \
    mu-docker-stats
