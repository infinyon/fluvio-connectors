# TARGET?=x86_64-unknown-linux-musl
# Build docker image for Fluvio.
ARCH=$(shell uname -m)
ifndef TARGET
ifeq ($(ARCH),arm64)
TARGET=aarch64-unknown-linux-musl
else
TARGET=x86_64-unknown-linux-musl
endif
endif

RUSTV?=stable
BUILD_PROFILE=$(if $(RELEASE),release,debug)
RELEASE_FLAG=$(if $(RELEASE),--release,)
TARGET_FLAG=$(if $(TARGET),--target $(TARGET),)
CARGO_BUILDER=$(if $(findstring arm,$(TARGET)),cross,cargo) # If TARGET contains the substring "arm"

# Connectors
TEST_CONNECTOR_BIN=$(if $(TARGET),./target/$(TARGET)/$(BUILD_PROFILE)/test-connector,./target/$(BUILD_PROFILE)/test-connector)
SYSLOG_BIN=$(if $(TARGET),./target/$(TARGET)/$(BUILD_PROFILE)/fluvio-syslog,./target/$(BUILD_PROFILE)/fluvio-syslog)

# These defaults are set for development purposes only. CI will override
CONNECTOR_NAME?=test-connector
CONNECTOR_VERSION=$(shell cargo metadata --format-version 1 | jq '.workspace_members[]' | sed 's/"//g' | awk '{if($$1 == "$(CONNECTOR_NAME)") print $$2}')
GIT_COMMIT=$(shell git rev-parse HEAD)
DOCKER_TAG=$(CONNECTOR_VERSION)-$(GIT_COMMIT)
CONNECTOR_PATH=$(shell cargo metadata --format-version 1 | jq '.workspace_members[]' | sed 's/"//g' | grep $(CONNECTOR_NAME) | awk '{print $$3}' | sed 's/(path+file:\/\///g' | sed 's/)//g')

#IMAGE_NAME?=infinyon/fluvio-connect-$(CONNECTOR_NAME)
CONNECTOR_BIN=$(if $(TARGET),./target/$(TARGET)/$(BUILD_PROFILE)/$(CONNECTOR_NAME),./target/$(BUILD_PROFILE)/$(CONNECTOR_NAME))

smoke-test:
	$(CARGO_BUILDER) run --bin fluvio-connector start ./test-connector/config.yaml

build:
	$(CARGO_BUILDER) build $(TARGET_FLAG) $(RELEASE_FLAG) --bin $(CONNECTOR_NAME)

ifeq (${CI},true)
# In CI, we expect all artifacts to already be built and loaded for the script
copy-binaries:
else
# When not in CI (i.e. development), build and copy the binaries alongside the Dockerfile
copy-binaries: build
	cp $(CONNECTOR_BIN) container-build
endif

official-containers: copy-binaries
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
