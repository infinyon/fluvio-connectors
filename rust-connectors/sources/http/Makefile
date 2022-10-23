test:
	bats ./tests/get-test-full-json.bats
	bats ./tests/get-time-test.bats
	bats ./tests/get-smartstream-test.bats
	bats ./tests/get-test.bats
	bats ./tests/get-test-json.bats
	bats ./tests/get-test-full.bats
	bats ./tests/post-test.bats

# this is for local development	only.  It derives from cats.yaml
start:
	cargo run --bin http-source --package http-source -- --fluvio-topic=cat-facts --endpoint=https://catfact.ninja/fact --interval=10s