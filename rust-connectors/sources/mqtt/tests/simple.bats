#!/usr/bin/env bats

setup() {
    FILE=$(mktemp)
    cp ./tests/test-config.yaml $FILE
    UUID=$(uuidgen | tr A-Z a-z)
    TOPIC=${UUID}-topic
    sed -i.BAK "s/mqtt-connector-name/${UUID}/g" $FILE
    fluvio connector create --config $FILE
}

teardown() {
    fluvio connector delete $UUID
    fluvio topic delete $TOPIC
}

@test "consume connector" {
    echo "TODO"
}

