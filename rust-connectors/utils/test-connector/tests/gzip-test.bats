#!/usr/bin/env bats

setup() {
    FILE=$(mktemp --suffix .yaml)
    UUID=$(uuidgen)
    TOPIC=${UUID}-topic
    cp ./tests/gzip-test-config.yaml $FILE

    sed -i.BAK "s/test-connector-name/${UUID}/g" $FILE
    fluvio topic create $TOPIC || true
    cargo run --bin connector-run --manifest-path ../../../Cargo.toml -- apply  --config $FILE
}

teardown() {
    cargo run --bin connector-run --manifest-path ../../../Cargo.toml -- delete  --config $FILE
    fluvio topic delete $TOPIC
}

@test "consume connector" {
    count=1
    echo "Starting consumer on topic $TOPIC"
    sleep 8

    fluvio consume -o 0 -d $TOPIC | while read input; do
        expected="Hello, Fluvio! - $count"
        echo $input = $expected
        [ "$input" = "$expected" ]
        count=$(($count + 1))
        if [ $count -eq 10 ]; then
            break;
        fi
    done

}


