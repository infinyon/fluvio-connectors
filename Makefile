# TARGET?=x86_64-unknown-linux-musl
# Build docker image for Fluvio.
ARCH=$(shell uname -m)

#ifndef TARGET
#TARGET=${ARCH}-unknown-linux-musl
#endif

RUSTV?=stable
BUILD_PROFILE=$(if $(RELEASE),release,debug)
RELEASE_FLAG=$(if $(RELEASE),--release,)
TARGET_FLAG=$(if $(TARGET),--target $(TARGET),)
CARGO_BUILDER=$(if $(findstring arm,$(TARGET)),cross,cargo) # If TARGET contains the substring "arm"


# These defaults are set for development purposes only. CI will override
CONNECTOR_VERSION=$(shell cargo metadata --format-version 1 | jq '.workspace_members[]' | sed 's/"//g' | awk '{if($$1 == "$(CONNECTOR_NAME)") print $$2}')
GIT_COMMIT=$(shell git rev-parse HEAD)
DOCKER_TAG=$(CONNECTOR_VERSION)-$(GIT_COMMIT)
CONNECTOR_PATH=$(shell cargo metadata --format-version 1 | jq '.workspace_members[]' | sed 's/"//g' | grep $(CONNECTOR_NAME) | awk '{print $$3}' | sed 's/(path+file:\/\///g' | sed 's/)//g')

#IMAGE_NAME?=infinyon/fluvio-connect-$(CONNECTOR_NAME)
CONNECTOR_BIN=$(if $(TARGET),./target/$(TARGET)/$(BUILD_PROFILE)/$(CONNECTOR_NAME),./target/$(BUILD_PROFILE)/$(CONNECTOR_NAME))

smoke-test:
	$(CARGO_BUILDER) run --bin fluvio-connector start ./test-connector/config.yaml

build:
	$(CARGO_BUILDER) build $(TARGET_FLAG) $(RELEASE_FLAG) --bin $(CONNECTOR_NAME) -p $(CONNECTOR_NAME)


# build all connectors
build-connector-sink-dynamodb:	build
build-connector-sink-dynamodb:	CONNECTOR_NAME=connector-sink-dynamodb

build-connector-sink-kafka:	build
build-connector-sink-kafka:	CONNECTOR_NAME=connector-sink-kafka

build-connector-sink-postgres:	build
build-connector-sink-postgres:	CONNECTOR_NAME=connector-sink-postgres

build-connector-sink-slack:	build
build-connector-sink-slack:	CONNECTOR_NAME=connector-sink-slack

build-connector-sink-sql:	build
build-connector-sink-sql:	CONNECTOR_NAME=connector-sink-sql

build-connector-source-http: build
build-connector-source-http: CONNECTOR_NAME=connector-source-http

build-connector-source-postgres: build
build-connector-source-postgres: CONNECTOR_NAME=connector-source-postgres

build-connector-source-syslog: build
build-connector-source-syslog: CONNECTOR_NAME=connector-source-syslog

build-connector-source-test: build
build-connector-source-test: CONNECTOR_NAME=connector-source-test

build-connector-source-mqtt: build
build-connector-source-mqtt: CONNECTOR_NAME=connector-source-mqtt



ifeq (${CI},true)
# In CI, we expect all artifacts to already be built and loaded for the script
copy-binaries:
else
# When not in CI (i.e. development), build and copy the binaries alongside the Dockerfile
copy-binaries: build
	cp $(CONNECTOR_BIN) container-build
endif


# Build docker image
# compute docker image target if not specified
# this is convienent for development only
ifndef TARGET
ifeq ($(ARCH),arm64)
docker-image: TARGET=aarch64-unknown-linux-musl
else
docker-image: TARGET=x86_64-unknown-linux-musl
endif
endif
# get image name
docker-image: IMAGE_NAME=$(shell cat crates/$(CONNECTOR_NAME)/IMAGE_NAME)
docker-image: build
	echo "Building connector $(CONNECTOR_BIN) on $(TARGET) image with tag: $(GIT_COMMIT) k8 type: $(K8_CLUSTER)"
	echo "Building docker image: $(IMAGE_NAME):$(DOCKER_TAG)"
	./build-scripts/docker/build-connector-image.sh $(CONNECTOR_NAME) $(CONNECTOR_BIN) $(TARGET) $(IMAGE_NAME)  $(K8_CLUSTER)


official-containers: build
	./build-scripts/docker/build-connector-image.sh

METADATA_OUT=metadata.json
metadata:
	cargo build
	echo '' >  $(METADATA_OUT)
	echo '[' >> $(METADATA_OUT)
	cargo run --bin mqtt -- metadata >> $(METADATA_OUT)
	echo ',' >> $(METADATA_OUT)
	cargo run --bin test-connector -- metadata >> $(METADATA_OUT)
	echo ']' >> $(METADATA_OUT)
	cat $(METADATA_OUT) | jq '.'


test:
	make -C $(CONNECTOR_PATH) test


unit-tests:
	cargo test --lib --all-features

check:
	cargo check --all --all-features --tests

check-clippy:
	cargo clippy

check-fmt:
	cargo fmt -- --check

clean:
	$(CARGO_BUILDER) clean
	rm -f container-build/test-connector
	rm -f container-build/fluvio-syslog

.EXPORT_ALL_VARIABLES:
FLUVIO_BUILD_ZIG ?= zig
FLUVIO_BUILD_LLD ?= lld
CC_aarch64_unknown_linux_musl=$(PWD)/build-scripts/aarch64-linux-musl-zig-cc
CC_x86_64_unknown_linux_musl=$(PWD)/build-scripts/x86_64-linux-musl-zig-cc
CXX_x86_64_unknown_linux_musl=$(PWD)/build-scripts/x86_64-linux-musl-zig-c++
CXX_aarch64_unknown_linux_musl=$(PWD)/build-scripts/aarch64-linux-musl-zig-c++
CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=$(PWD)/build-scripts/ld.lld
CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=$(PWD)/build-scripts/ld.lld
DOCKER_TAG=$(CONNECTOR_VERSION)-$(GIT_COMMIT)
