#!/usr/bin/env bats

setup() {
    #export TOPIC
    FILE=$(mktemp --suffix .yaml)
    UUID=$(uuidgen)
    echo $UUID
    TOPIC=longevity
    fluvio topic create $TOPIC || true
    STARTING_OFFSET=$(fluvio partition list | grep $TOPIC | awk '{print $9}')
    export STARTING_OFFSET
    cp ./tests/test-mode-config.yaml $FILE

    sed -i.BAK "s/test-connector-name/${UUID}/g" $FILE
    fluvio connector create --config $FILE
}

teardown() {
    fluvio connector delete $UUID-fluvio-test
    fluvio topic delete $TOPIC
}

@test "Check fluvio-test producing data" {
    echo "Waiting a moment for fluvio-test to write to topic $TOPIC"
    sleep 30 

    ENDING_OFFSET=$(fluvio partition list | grep $TOPIC | awk '{print $9}')
    echo "Starting offset: $STARTING_OFFSET"
    echo "Ending offset: $ENDING_OFFSET"
    [ "0" -ne "$ENDING_OFFSET" ]
    [ "$STARTING_OFFSET" -ne "$ENDING_OFFSET" ]
}

