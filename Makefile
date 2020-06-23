ARTIFACT=map-lambda-export-s3
ARCHIVE=$(ARTIFACT).zip
VERSION=$(BUILD_NUMBER)
BUILD_DIR=build
REQUIREMENTS_DIR=${BUILD_DIR}/requirements

clean:
	-rm -rf $(BUILD_DIR)
	-rm map-lambda-export-s3.zip

test:
	./pytest_in_docker.sh

build:
	./pybuild_in_docker.sh map-lambda-export-s3

publish:
	curl -m 300 \
		-u "$(ORG_GRADLE_PROJECT_nexusUsername):$(ORG_GRADLE_PROJECT_nexusPassword)" \
		-T "./$(ARCHIVE)" \
		"$(ORG_GRADLE_PROJECT_repositoryReleaseUrl)/com/bancvue/$(ARTIFACT)/$(VERSION)/$(ARTIFACT).zip"

ci: clean test build publish

.PHONY: test