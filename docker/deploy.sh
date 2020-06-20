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
echo "from bout_runners.runner.bout_runner import BoutRunner" >> test.py
echo "from bout_runners.metadata.status_checker import StatusChecker" >> test.py
echo "from bout_runners.metadata.metadata_reader import MetadataReader" >> test.py
echo "BoutRunner().run()" >> test.py
echo "status_checker = StatusChecker()" >> test.py
echo "status_checker.check_and_update_status()" >> test.py
echo "metadata_reader = MetadataReader()" >> test.py
echo "metadata = metadata_reader.get_all_metadata()" >> test.py
echo "print(metadata)" >> test.py
docker run "$IMAGE":"$VERSION" -c 'cd $HOME/BOUT-dev/examples/conduction && python test.py'
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
