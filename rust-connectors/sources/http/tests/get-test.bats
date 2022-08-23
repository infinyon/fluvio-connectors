#!/usr/bin/env bats

setup() {
    cargo build -p http-json-mock
    ../../../target/debug/http-json-mock & disown
    MOCK_PID=$!
    FILE=$(mktemp --suffix .yaml)
    cp ./tests/get-test-config.yaml $FILE
    UUID=$(uuidgen)
    TOPIC=${UUID}-topic
    fluvio topic create $TOPIC || true

    sed -i.BAK "s/http-json-connector/${UUID}/g" $FILE
    IP_ADDRESS=$(ip route get 8.8.8.8 | awk -F"src " 'NR==1{split($2,a," ");print a[1]}')
    sed -i.BAK "s/IP_ADDRESS/${IP_ADDRESS}/g" $FILE
    cargo run --bin connector-deploy --manifest-path ../../../Cargo.toml -- apply  --config $FILE
}

teardown() {
    cargo run --bin connector-deploy --manifest-path ../../../Cargo.toml -- delete  --config $FILE
    fluvio topic delete $TOPIC
    kill $MOCK_PID
}

@test "http-connector-get-test" {
    count=1
    echo "Starting consumer on topic $TOPIC"
    sleep 13

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

