# Running the Slack Github Connector

* Clone this repo, navigate to `examples/github-stars`.
* Modify `slack-connector.yaml` to have a valid [slack webhook
url](https://api.slack.com/messaging/webhooks) in the `webhook-url` key..
* Modify `http-connector.yaml` to have a valid [github personal access
token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token)
where it says `<YOUR GITHUB TOKEN HERE>`


## Long instructions

### Compile and load the smart modules
* `cargo build -p github-stars-smartmodule-map --target wasm32-unknown-unknown --release`
* `cargo build -p slack-display-smartmodule-map --target wasm32-unknown-unknown --release`
* `fluvio sm create star-selector --wasm-file ../../target/wasm32-unknown-unknown/release/github_stars_smartmodule_map.wasm`
* `fluvio sm create star-display --wasm-file ../../target/wasm32-unknown-unknown/release/slack_display_smartmodule_map.wasm`

Alternatively:
`make smart-module-create` does those four commands.

### Create the connectors
* `cargo run --bin connector-run --manifest-path ../../Cargo.toml -- apply  -c ./slack-connector.yaml`
* `cargo run --bin connector-run  --manifest-path ../../Cargo.toml -- apply  -c ./http-connector.yaml`

## Simple instructions
* If you feel like doing no work, `make all` will:
    - compile the `aggregate` smartmodule for the http connector
    - compile the `filter_map` smartmodule for the slack connector
    - Add the smartmodules to fluvio vio `fluvio smartmodule create` with the appropriate names and locations.
    - `cargo run --bin connector-run --manifest-path ../../Cargo.toml -- apply  -c ./http-connector.yaml`
    - `cargo run --bin connector-run --manifest-path ../../Cargo.toml -- apply  -c ./slack-connector.yaml`
* Similar to this `make clean` should destroy all
