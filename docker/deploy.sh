#!/usr/bin/env bash

# Exit on error
# NOTE: Some consider trap 'do something' ERR a better practice
# https://stackoverflow.com/a/19622569/2786884
set -e

NAME=bout_runners
IMAGE=loeiten/"$NAME"
VERSION=$(sed -n "s/__version__ = ['\"]\([^'\"]*\)['\"]/\1/p" ${NAME}/__init__.py)_$(date +%Y%m%d)
docker build -f docker/Dockerfile -t "$IMAGE":"$VERSION" .
# NOTE: DOCKER_PASSWORD and DOCKER_USERNAME are environment secrets of
#       the github repo
echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
docker push "$IMAGE":"$VERSION"
docker tag "$IMAGE":"$VERSION" "$IMAGE":latest
docker push "$IMAGE":latest
