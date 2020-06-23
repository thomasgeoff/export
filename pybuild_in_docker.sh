#!/bin/bash
if [ $# != 1 ]; then
  echo "Usage : $0 ARTIFACT"
  exit 1
fi
ARTIFACT=${1}

# docker container setup to run tests
docker pull python:3.6

docker rm -f ${ARTIFACT}_MAIN_C || echo "it's dead jim"

docker run --name ${ARTIFACT}_MAIN_C \
  -v "$PWD":/usr/src/${ARTIFACT} \
  -w /usr/src/${ARTIFACT} \
  -i python:3.6 sh -c " \uname -a; pwd; ls; apt-get update; apt-get -y install zip; /usr/src/${ARTIFACT}/makezip.sh ${ARTIFACT}"
 
docker rm -f ${ARTIFACT}_MAIN_C

exit $dockerExitCode