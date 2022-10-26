#!/usr/bin/env bats

setup() {
    cargo build -p http-json-mock

    ../../../target/debug/http-json-mock & disown

    MOCK_PID=$!
    FILE=$(mktemp --suffix .yaml)
    cp ./tests/get-smartstream-config.yaml $FILE
    UUID=$(uuidgen)
    TOPIC=${UUID}-topic
    fluvio topic create $TOPIC || true

    MODULE=${UUID}-map
    cd ../../utils/fluvio-smartstream-map
    smdk build
    smdk load
    cd - 

    sed -i.BAK "s/http-json-connector/${UUID}/g" $FILE

    IP_ADDRESS=$(ip route get 8.8.8.8 | awk -F"src " 'NR==1{split($2,a," ");print a[1]}')
    sed -i.BAK "s/IP_ADDRESS/${IP_ADDRESS}/g" $FILE
    cargo run --bin connector-run --manifest-path ../../../Cargo.toml -- apply  --config $FILE
}

teardown() {
    cargo run --bin connector-run --manifest-path ../../../Cargo.toml -- delete  --config $FILE
    fluvio topic delete $TOPIC
    fluvio smart-module delete $MODULE
    fluvio sm delete infinyon/map-uppercase@0.1.0
    kill $MOCK_PID
}

@test "http-connector-get-smartmodule test" {
    count=1
    echo "Starting consumer on topic $TOPIC"
    sleep 13

    fluvio consume -o 0 -d $TOPIC | while read input; do
        expected="HELLO, FLUVIO! - $count"
        echo $input = $expected
        [ "$input" = "$expected" ]
        count=$(($count + 1))
        if [ $count -eq 10 ]; then
            break;
        fi
    done

}

