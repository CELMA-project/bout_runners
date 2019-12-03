#!/usr/bin/env bash

VERSION=$(sed -n "s/__version__ = ['\"]\([^'\"]*\)['\"]/\1/p" bout_install/__init__.py)_$(date +%Y%m%d)
IMAGE=loeiten/bout_runners
docker build -f docker/Dockerfile -t "$IMAGE":"$VERSION" .
# NOTE: DOCKER_PASSWORD and DOCKER_USERNAME are environment secrets of
#       the github repo
echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
docker push "$IMAGE":"$VERSION"
docker tag "$IMAGE":"$VERSION" "$IMAGE"/bout_dev:latest
docker push "$IMAGE":latest