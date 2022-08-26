#!/usr/bin/env bats

load '../../../utils/bats-helpers/bats-support/load'
load '../../../utils/bats-helpers/bats-assert/load'

setup() {
    cargo build -p http-json-mock
    ../../../target/debug/http-json-mock & disown
    MOCK_PID=$!
    FILE=$(mktemp --suffix .yaml)
    cp ./tests/get-test-full-config.yaml $FILE
    UUID=$(uuidgen)
    TOPIC=${UUID}-topic
    fluvio topic create $TOPIC || true

    sed -i.BAK "s/http-json-connector/${UUID}/g" $FILE
    IP_ADDRESS=$(ip route get 8.8.8.8 | awk -F"src " 'NR==1{split($2,a," ");print a[1]}')
    sed -i.BAK "s/IP_ADDRESS/${IP_ADDRESS}/g" $FILE
    cargo run --bin connector-run --manifest-path ../../../Cargo.toml -- apply  --config $FILE
}

teardown() {
    cargo run --bin connector-run --manifest-path ../../../Cargo.toml -- delete  --config $FILE
    fluvio topic delete $TOPIC
    kill $MOCK_PID
}

@test "http-connector-get-full-test" {
    count=1
    echo "Starting consumer on topic $TOPIC"
    sleep 10

    run fluvio consume -o 0 --end-offset 0 -d $TOPIC
    assert_output --partial 'HTTP/1.1 200 OK'

    run fluvio consume -o 0 --end-offset 0 -d $TOPIC
    assert_output --partial 'content-type: text/plain;charset=utf-8'

    run fluvio consume -o 1 --end-offset 1 -d $TOPIC
    assert_output --partial 'Hello, Fluvio! - ' 
}

