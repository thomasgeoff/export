#!/usr/bin/env bash

export PYTHONPATH=.

pycodestyle ./*.py --show-source --ignore=E501,E116

# sleep 120

./flyway-4.0.3/flyway migrate -quiet -user=root -password=cimysql_password -url=jdbc:mysql://db-host/flyway -locations=filesystem:/app/map-data-pipeline-flyway/src/s3files/sql/

pytest
