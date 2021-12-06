#!/usr/bin/env bats

setup() {
    cargo build -p http-json-mock
    cargo build -p fluvio-wasm-map --target wasm32-unknown-unknown --release

    ../../../target/debug/http-json-mock & disown

    MOCK_PID=$!
    FILE=$(mktemp --suffix .yaml)
    cp ./tests/get-smartstream-config.yaml $FILE
    UUID=$(uuidgen)
    TOPIC=${UUID}-topic

    MODULE=${UUID}-map
    fluvio smart-module create $MODULE --wasm-file ../../../target/wasm32-unknown-unknown/release/fluvio_wasm_map.wasm

    sed -i.BAK "s/http-json-connector/${UUID}/g" $FILE

    IP_ADDRESS=$(ip route get 8.8.8.8 | awk -F"src " 'NR==1{split($2,a," ");print a[1]}')
    sed -i.BAK "s/IP_ADDRESS/${IP_ADDRESS}/g" $FILE
    fluvio connector create --config $FILE
}

teardown() {
    fluvio connector delete $UUID
    fluvio topic delete $TOPIC
    fluvio smart-module delete $MODULE
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

