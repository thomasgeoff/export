ARTIFACT=map_lambda_export_s3
DB_USER=root
DB_PASSWORD=cimysql_password
DB_HOST=db-host
DB_PORT=53306
DB_SCHEMA=flyway

docker rm ${ARTIFACT}_db_container || echo "it's dead jim"

# docker container setup to run tests
docker build -t "${ARTIFACT}_container:test" -f Dockerfile.test .

# create mysql container
docker run --name ${ARTIFACT}_db_container \
  -e "MYSQL_USER=cimysql" \
  -e "MYSQL_PASSWORD=${DB_PASSWORD}" \
  -e "MYSQL_ROOT_PASSWORD=${DB_PASSWORD}" \
  -e "MYSQL_DATABASE=${DB_SCHEMA}" \
  -p ${DB_PORT}:3306 \
  -d mysql:5.7 --max-connections=20 --sql-mode="NO_ENGINE_SUBSTITUTION"

# sleep in case the mysql container isn't up and running yet
sleep 20

# start python container and link in the mysql container
docker run --rm --name ${ARTIFACT}_container \
  --link "${ARTIFACT}_db_container:${DB_HOST}" \
  --env-file=.env.test \
  -i "${ARTIFACT}_container:test" sh -c ./run_tests.sh

dockerExitCode=$?

# docker kill ${ARTIFACT}_db_container
docker kill ${ARTIFACT}_db_container
docker rm ${ARTIFACT}_db_container

exit $dockerExitCode