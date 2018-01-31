#!/usr/bin/env bash

curl -H "Content-Type: application/json" --data '{"build": true}' -X POST https://registry.hub.docker.com/u/johejo/sphttp-proxy/trigger/${DOCKER_HUB_TRIGGER_TOKEN}/