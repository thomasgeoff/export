REQUIREMENTS_DIR=build
ARCHIVE=${1}.zip

# remove archive if it already exists
rm ${ARCHIVE} 2> /dev/null

mkdir -p ${REQUIREMENTS_DIR}
# Install requirements
pip install -t ${REQUIREMENTS_DIR} -r requirements.txt
# -D states not to include directories
# -r recursive
# -9 slowest compression
# -g grow the archive
# remove unecessary items from installation
rm -r ${REQUIREMENTS_DIR}/*.dist-info
rm -r ${REQUIREMENTS_DIR}/*.pyc
rm -r ${REQUIREMENTS_DIR}/**/*.pyc
# Leave off the botocore and boto3 libraries which will be included in the kasasa_common but are provided by AWS
rm -r ${REQUIREMENTS_DIR}/boto3
rm -r ${REQUIREMENTS_DIR}/botocore
rm -r ${REQUIREMENTS_DIR}/s3transfer
rm -r ${REQUIREMENTS_DIR}/concurrent

zip -9 ${ARCHIVE} sqls/*.sql
zip -9 ${ARCHIVE} *.py
zip -9 ${ARCHIVE} *.pem
zip -9 ${ARCHIVE} *.json
# Change to the requirements directory to include the requirements
# in the built artifact for the lambda.
cd ${REQUIREMENTS_DIR}; zip -r9 ../${ARCHIVE} * --exclude '*.pyc'