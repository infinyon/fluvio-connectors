TARGET?=x86_64-unknown-linux-musl
RUSTV?=stable
BUILD_PROFILE=$(if $(RELEASE),release,debug)
RELEASE_FLAG=$(if $(RELEASE),--release,)
TARGET_FLAG=$(if $(TARGET),--target $(TARGET),)

# Connectors
TEST_CONNECTOR_BIN=$(if $(TARGET),./target/$(TARGET)/$(BUILD_PROFILE)/test-connector,./target/$(BUILD_PROFILE)/test-connector)
SYSLOG_BIN=$(if $(TARGET),./target/$(TARGET)/$(BUILD_PROFILE)/fluvio-syslog,./target/$(BUILD_PROFILE)/fluvio-syslog)

smoke-test:
	cargo run --bin fluvio-connector start ./test-connector/config.yaml

build:
	cargo build $(TARGET_FLAG) $(RELEASE_FLAG) 

official-containers: build
	cp $(TEST_CONNECTOR_BIN) container-build
	cd container-build && \
		docker build -t infinyon/fluvio-connect-test-connector --build-arg CONNECTOR_NAME=test-connector .

	cp $(SYSLOG_BIN) container-build 
	cd container-build && \
		docker build -t infinyon/fluvio-connect-syslog --build-arg CONNECTOR_NAME=fluvio-syslog .

clean:
	cargo clean
	rm -f container-build/test-connector
	rm -f container-build/fluvio-syslog