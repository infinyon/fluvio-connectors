TARGET?=x86_64-unknown-linux-musl
RUSTV?=stable
BUILD_PROFILE=$(if $(RELEASE),release,debug)
RELEASE_FLAG=$(if $(RELEASE),--release,)
TARGET_FLAG=$(if $(TARGET),--target $(TARGET),)

# Connectors
TEST_CONNECTOR_BIN=$(if $(TARGET),./target/$(TARGET)/$(BUILD_PROFILE)/test-connector,./target/$(BUILD_PROFILE)/test-connector)
SYSLOG_BIN=$(if $(TARGET),./target/$(TARGET)/$(BUILD_PROFILE)/fluvio-syslog,./target/$(BUILD_PROFILE)/fluvio-syslog)

# These defaults are set for development purposes only. CI will override
CONNECTOR_NAME?=test-connector
IMAGE_NAME?=infinyon/fluvio-connect-test-connector

smoke-test:
	cargo run --bin fluvio-connector start ./test-connector/config.yaml

ifndef CONNECTOR_NAME
build:
	cargo build $(TARGET_FLAG) $(RELEASE_FLAG) 
else
build:
	cargo build $(TARGET_FLAG) $(RELEASE_FLAG) --bin $(CONNECTOR_NAME)
endif

ifeq (${CI},true)
# In CI, we expect all artifacts to already be built and loaded for the script
copy-binaries:
else
# When not in CI (i.e. development), build and copy the binaries alongside the Dockerfile 
copy-binaries: build 
	cp $(TEST_CONNECTOR_BIN) container-build
	cp $(SYSLOG_BIN) container-build 
endif

official-containers: copy-binaries
	cd container-build && \
		docker build -t $(IMAGE_NAME) --build-arg CONNECTOR_NAME=$(CONNECTOR_NAME) .

clean:
	cargo clean
	rm -f container-build/test-connector
	rm -f container-build/fluvio-syslog