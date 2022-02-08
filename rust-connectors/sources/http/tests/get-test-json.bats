#!/usr/bin/env bats

load '../../../utils/bats-helpers/bats-support/load'
load '../../../utils/bats-helpers/bats-assert/load'

setup() {
    cargo build -p http-json-mock
    ../../../target/debug/http-json-mock & disown
    MOCK_PID=$!
    FILE=$(mktemp --suffix .yaml)
    cp ./tests/get-test-json-config.yaml $FILE
    UUID=$(uuidgen)
    TOPIC=${UUID}-topic

    sed -i.BAK "s/http-json-connector/${UUID}/g" $FILE
    IP_ADDRESS=$(ip route get 8.8.8.8 | awk -F"src " 'NR==1{split($2,a," ");print a[1]}')
    sed -i.BAK "s/IP_ADDRESS/${IP_ADDRESS}/g" $FILE
    fluvio connector create --config $FILE
}

teardown() {
    fluvio connector delete $UUID
    fluvio topic delete $TOPIC
    kill $MOCK_PID
}

@test "http-connector-get-json-test" {
    count=1
    echo "Starting consumer on topic $TOPIC"
    sleep 10

    run fluvio consume -o 1 --end-offset 1 -d $TOPIC
    assert_output --partial '{"body":"Hello, Fluvio! - ' 
}

