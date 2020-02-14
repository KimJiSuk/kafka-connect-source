.PHONY: *

HELP_TAB_WIDTH = 25

.DEFAULT_GOAL := help

SHELL=/bin/bash -o pipefail

check-dependency = $(if $(shell command -v $(1)),,$(error Make sure $(1) is installed))

check-dependencies:
	@#(call check-dependency,mvn)
	@#(call check-dependency,docker)
	@#(call check-dependency,grep)
	@#(call check-dependency,cut)
	@#(call check-dependency,sed)

CP_VERSION ?= 5.3.1
KAFKA_CONNECT_VERSION ?= 0.1.0
AGGREGATE_VERSION = $(KAFKA_CONNECT_VERSION)-$(CP_VERSION)

KAFKA_CONNECT_LOCAL_VERSION = $(shell make local-package-version)
AGGREGATE_LOCAL_VERSION = $(KAFKA_CONNECT_LOCAL_VERSION)-$(CP_VERSION)

help:
	@$(foreach m,$(MAKEFILE_LIST),grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(m) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-$(HELP_TAB_WIDTH)s\033[0m %s\n", $$1, $$2}';)

local-package-version: check-dependencies ## Retrieves the jar version from the maven project definition
	@mvn help:evaluate -Dexpression=project.version -q -DforceStdout

package: check-dependencies ## Creates the assembly jar
	@mvn clean package

build-docker-from-local: check-dependencies package ## Build the Docker image using the locally mvn built kafka-connect package
	@docker build -t kafka-connect:$(AGGREGATE_LOCAL_VERSION) --build-arg KAFKA_CONNECT_VERSION=$(KAFKA_CONNECT_LOCAL_VERSION) --build-arg CP_VERSION=$(CP_VERSION) -f Dockerfile .
