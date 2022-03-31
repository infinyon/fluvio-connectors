
all: slack-connector http-connector

GH_SM=../../target/wasm32-unknown-unknown/release/github_stars_smartmodule_map.wasm
GH_SM_NAME=star-selector
SLACK_SM=../../target/wasm32-unknown-unknown/release/slack_display_smartmodule_map.wasm
SLACK_SM_NAME=star-display

smart-module-compile:
	cargo build -p github-stars-smartmodule-map --target wasm32-unknown-unknown --release
	cargo build -p slack-display-smartmodule-map --target wasm32-unknown-unknown --release

smart-module-create: smart-module-compile
	fluvio sm create $(GH_SM_NAME) --wasm-file $(GH_SM)
	fluvio sm create $(SLACK_SM_NAME) --wasm-file $(SLACK_SM)

slack-connector: smart-module-create
	fluvio connector create -c ./slack-connector.yaml

http-connector: smart-module-create
	fluvio connector create -c ./http-connector.yaml



clean:
	fluvio sm delete $(GH_SM_NAME) || true
	fluvio sm delete $(SLACK_SM_NAME) || true
	fluvio connector delete github-stars-output || true
	fluvio connector delete github-stars-input || true
	fluvio topic delete github-stars || true
