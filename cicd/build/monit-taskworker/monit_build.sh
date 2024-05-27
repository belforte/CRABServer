#!/bin/bash

## bash ./cicd/build/monit-taskworker/monit_build.sh v3.240111

TAG=pypi-test12-1716447194

docker build \
  --build-arg "TAG=$TAG" \
  --network=host \
  -t registry.cern.ch/cmscrab/crabtaskworker:$TAG-monittw \
  ./ \
  -f ./cicd/build/monit-taskworker/Dockerfile

