#!/usr/bin/env bats

setup() {
    FILE=$(mktemp --suffix .yaml)
    cp ./tests/test-config.yaml $FILE
    UUID=$(uuidgen)
    TOPIC=${UUID}-topic
    sed -i.BAK "s/mqtt-connector-name/${UUID}/g" $FILE
    fluvio topic create $TOPIC || true
    cargo run --bin connector-run --manifest-path ../../../Cargo.toml -- apply  --config $FILE
}

teardown() {
    cargo run --bin connector-run --manifest-path ../../../Cargo.toml -- delete  --config $FILE
    fluvio topic delete $TOPIC
}

@test "consume connector" {
    echo "TODO"
}

