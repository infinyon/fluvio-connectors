#!/usr/bin/env bats

setup() {
    FILE=$(mktemp)
    UUID=$(uuidgen | tr A-Z a-z)
    TOPIC=${UUID}-topic
    cp ./tests/test-config.yaml $FILE
    sed -i.BAK "s/hello-connector-name/${UUID}/g" $FILE
    cat $FILE
    fluvio connector create --config $FILE
}

teardown() {
    fluvio connector delete $UUID
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

