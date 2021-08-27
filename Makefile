smoke-test:
	cargo run --bin fluvio-connector start ./test-connector/config.yaml

official-containers:
	cargo build --target x86_64-unknown-linux-musl --release
	docker build -t fluvio-connector-test-connector --build-arg CONNECTOR_NAME=test-connector .
	docker build -t fluvio-connector-fluvio-syslog --build-arg CONNECTOR_NAME=fluvio-syslog .