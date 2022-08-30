#!/usr/bin/env bats

setup() {
    cargo build -p http-json-mock
    ../../../target/debug/http-json-mock & disown
    MOCK_PID=$!
    FILE=$(mktemp --suffix .yaml)
    cp ./tests/get-time-test-config.yaml $FILE
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

@test "http-connector-get-time-test" {
    MAX_MS_FOR_5_RECORDS=600
    echo "Starting consumer on topic $TOPIC"
    echo "This test ensures that with a http interval of 100ms, and 0ms source-linger time, 5 records should be produces in under ${MAX_MS_FOR_5_RECORDSH}ms"
    sleep 13

    fluvio consume -T 5 -d $TOPIC | while read line; do
        difference=$((($(date +%s%N) - $line)/1000000))
        echo $difference
        [ $difference -lt ${MAX_MS_FOR_5_RECORDS} ]
    done
}

