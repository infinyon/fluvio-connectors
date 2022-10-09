#!/usr/bin/env bats
#
load '../../../utils/bats-helpers/bats-support/load'
load '../../../utils/bats-helpers/bats-assert/load'

setup() {
    make postgres
    sleep 1
    make postgres-tables
    cargo build --target wasm32-unknown-unknown -p json-sql --profile release-lto
    fluvio sm create --wasm-file ../../../target/wasm32-unknown-unknown/release-lto/json_sql.wasm json-sql-bats-simple-test
    cargo build -p sql-sink
    RUST_BACKTRACE=full RUST_LOG=info ../../../target/debug/sql-sink --database-url postgres://fluvio:fluviopassword@127.0.0.1:5433 --fluvio-topic fluvio-sql-sink --transform '{"uses":"json-sql-bats-simple-test","invoke":"insert","with":"{\"table\":\"messages\",\"map-columns\":{\"message\":{\"json-key\":\"payload.message.text\",\"value\":{\"type\":\"text\"}},\"id\":{\"json-key\":\"payload.message.id\",\"value\":{\"type\":\"int4\"}}}}"}' & disown
    SQL_PID=$!
    sleep 1
}

teardown() {
    make postgres-stop
    fluvio sm delete json-sql-bats-simple-test
    kill $SQL_PID
    fluvio topic delete fluvio-sql-sink
}

@test "produce connector" {
    echo '{"mqtt_topic":"233d6e8b-c37a-441b-b27c-caedfeb90872","payload":{"message":{"id":42,"text":"hi"}}}' | fluvio produce fluvio-sql-sink
    sleep 1
    PGPASSWORD=fluviopassword run docker exec -it fluvio-sql-sink psql -U fluvio fluvio -c 'select * from messages;'
    assert_line --index 2 --partial " hi      | 42"
}
