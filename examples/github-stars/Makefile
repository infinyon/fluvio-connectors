
all: slack-connector http-connector

GH_SM=../../target/wasm32-unknown-unknown/release/github_stars_smartmodule_map.wasm
GH_SM_NAME=star-selector

smart-module-compile:
	cargo build -p github-stars-smartmodule-map --target wasm32-unknown-unknown --release

smart-module-create: smart-module-compile
	fluvio sm create $(GH_SM_NAME) --wasm-file $(GH_SM)

slack-connector: smart-module-create
	fluvio connector create -c ./slack-connector.yaml

http-connector: smart-module-create
	fluvio connector create -c ./http-connector.yaml



clean:
	fluvio sm delete $(GH_SM_NAME);
	fluvio connector delete github-stars-output
	fluvio connector delete github-stars-input
	fluvio topic delete github-stars
