#!/usr/bin/env bats

setup() {
    FILE=$(mktemp --suffix .yaml)
    cp ./tests/test-config.yaml $FILE
    UUID=$(uuidgen)
    TOPIC=${UUID}-topic
    sed -i.BAK "s/mqtt-connector-name/${UUID}/g" $FILE
    fluvio topic create $TOPIC
    fluvio connector create --config $FILE
}

teardown() {
    fluvio connector delete $UUID
    fluvio topic delete $TOPIC
}

@test "consume connector" {
    echo "TODO"
}

