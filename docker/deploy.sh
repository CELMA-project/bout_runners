#!/usr/bin/env bash

# Exit on error
# NOTE: Some consider trap 'do something' ERR a better practice
# https://stackoverflow.com/a/19622569/2786884
set -e

printf "\nObtaining version\n"
NAME=bout_runners
VERSION=$(sed -n "s/__version__ = ['\"]\([^'\"]*\)['\"]/\1/p" ${NAME}/__init__.py)_$(date +%Y%m%d)
IMAGE=loeiten/"$NAME"
printf '\nCurrent version %s\n' "$VERSION"
printf "\nBuilding image\n"
docker build -f docker/Dockerfile -t "$IMAGE":"$VERSION" .
printf "\nTesting build\n"
# Test that the build is working
# NOTE: The /bin/sh is already stated in the ENTRYPOINT
CHANGE_DIR='cd $HOME/BOUT-dev/examples/conduction'
PYTHON_TEST='python -c "'\
'from bout_runners.runner.bout_runner import BoutRunner; '\
'from bout_runners.metadata.status_checker import StatusChecker; '\
'from bout_runners.metadata.metadata_reader import MetadataReader; '\
'BoutRunner().run(); '\
'status_checker = StatusChecker(); '\
'status_checker.check_and_update_status(); '\
'metadata_reader = MetadataReader(); '\
'metadata = metadata_reader.get_all_metadata(); '\
'print(metadata)"'
docker run --rm "$IMAGE":"$VERSION" -c "$CHANGE_DIR&&$PYTHON_TEST"
printf "\nTest Passed\n"
# NOTE: DOCKER_PASSWORD and DOCKER_USERNAME are environment secrets of
#       the github repo
echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
printf '\nPushing %s\n' "$IMAGE:$VERSION"
docker push "$IMAGE":"$VERSION"
docker tag "$IMAGE":"$VERSION" "$IMAGE":latest
printf '\nPushing %s\n' "$IMAGE:$VERSION"
docker push "$IMAGE":latest
printf "\nSuccess\n"
