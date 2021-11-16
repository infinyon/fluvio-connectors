#!/usr/bin/env bats

export PGPASSWORD=mysecretpassword

setup_postgres() {
    kubectl apply -f ./postgres.yml
    sleep 60

    psql -h "$(minikube ip)" -U postgres -p5432 -c "SELECT pg_create_logical_replication_slot('fluvio', 'pgoutput');" || true
    psql -h "$(minikube ip)" -U postgres -p5432 -c "CREATE PUBLICATION fluvio FOR ALL TABLES;" || true
}

setup() {
    setup_postgres

    FILE="$(mktemp -d)/simple.yml"
    echo "Current directory: $(pwd)" >&2
    cp ./tests/simple.yml $FILE
    UUID=$(uuidgen | tr '[:upper:]' '[:lower:]')
    echo "Got UUID $UUID" >&2
    TOPIC=${UUID}-topic
    echo "Got TOPIC $TOPIC" >&2
    sed -i.BAK "s/postgres-connector-name/${UUID}/g" $FILE
    fluvio topic create $TOPIC
    fluvio connector create --config $FILE
}

teardown() {
    fluvio connector delete $UUID && echo "Connector deleted: $UUID" >&2
    fluvio topic delete $TOPIC

    kubectl delete pod postgres-leader
    kubectl delete svc postgres-leader-service
    kubectl delete configmap postgres-conf
}

psql_simple() {
    psql -h "$(minikube ip)" -U postgres -p5432 -f ./tests/simple.sql
}

assert_simple() {
    # Use bats "run" to put contents into "lines" bash array
    run cat ./tests/consumed.txt

    # Test the expected message "type"s
    expected_types=( "begin" "commit" "begin" "commit" "begin" "relation" "insert" "commit" "begin" "insert" "insert" "commit" )
    for i in ${!lines[*]}
    do
        echo "Index $i"
        line=${lines[$i]}
        type="$(echo -n "${line}" | jq -r '.message.type')"
        echo "Type $i is ${type}"

        echo "Testing ${type} = ${expected_types[$i]}"
        [ "${type}" = "${expected_types[$i]}" ]
    done

    # Test the relation message
    line=${lines[5]}
    namespace="$(echo -n "${line}" | jq -r '.message.namespace')"
    name="$(echo -n "${line}" | jq -r '.message.name')"
    col_names="$(echo -n "${line}" | jq -c '[.message.columns[].name]')"
    col_types="$(echo -n "${line}" | jq -c '[.message.columns[].type_id]')"
    echo "Relation namespace: ${namespace}"
    echo "Relation name: ${name}"
    echo "Relation column names: ${col_names}"
    echo "Relation column types: ${col_types}"
    [ "${namespace}" = "public" ]
    [ "${name}" = "pets" ]
    [ "${col_names}" = '["id","name","species","birth"]' ]
    [ "${col_types}" = '[23,1043,1043,1082]' ]

    # Use rel_id to check tuples from INSERT statements following
    rel_id="$(echo -n "${line}" | jq -r '.message.rel_id')"

    # Test the first insert message
    line=${lines[6]}
    insert_rel_id="$(echo -n "${line}" | jq -r '.message.rel_id')"
    insert_tuple="$(echo -n "${line}" | jq -c '.message.tuple')"
    echo "Insert rel_id: ${insert_rel_id}"
    echo "Insert tuple: ${insert_tuple}"
    # Check that this INSERT has the same rel_id as the relation message before
    [ "${insert_rel_id}" = "${rel_id}" ]
    [ "${insert_tuple}" = '[{"Int4":1},{"String":"Polly"},{"String":"Parrot"},{"RawText":[50,48,50,48,45,48,49,45,48,49]}]' ]

    # Test the second insert message
    line=${lines[9]}
    insert_rel_id="$(echo -n "${line}" | jq -r '.message.rel_id')"
    insert_tuple="$(echo -n "${line}" | jq -c '.message.tuple')"
    echo "Insert rel_id: ${insert_rel_id}"
    echo "Insert tuple: ${insert_tuple}"
    # Check that this INSERT has the same rel_id as the relation message before
    [ "${insert_rel_id}" = "${rel_id}" ]
    [ "${insert_tuple}" = '[{"Int4":2},{"String":"Ginger"},{"String":"Dog"},{"RawText":[50,48,49,53,45,48,53,45,48,57]}]' ]

    # Test the third insert message
    line=${lines[10]}
    insert_rel_id="$(echo -n "${line}" | jq -r '.message.rel_id')"
    insert_tuple="$(echo -n "${line}" | jq -c '.message.tuple')"
    echo "Insert rel_id: ${insert_rel_id}"
    echo "Insert tuple: ${insert_tuple}"

    # Check that this INSERT has the same rel_id as the relation message before
    [ "${insert_rel_id}" = "${rel_id}" ]
    [ "${insert_tuple}" = '[{"Int4":3},{"String":"Spice"},{"String":"Dog"},{"RawText":[50,48,49,53,45,48,53,45,48,57]}]' ]
}

# Test that we can consume events created by the connector
# We put the contents of the topic into ./tests/consumed.txt,
# then run assertions on it
# @test "consume connector" {
#     fluvio consume ${UUID}-topic -B > ./tests/consumed.txt &
#     TASK_PID=$!
#     psql_simple &
#     sleep 10
#     kill $TASK_PID
#
#     assert_simple
# }

setup_smartmodule() {
    SM_DIR="../../utils/fluvio-postgres-map"
    rustup target add wasm32-unknown-unknown
    (cd "${SM_DIR}"; cargo build --release)
    fluvio smartmodule create postgres-map --wasm-file="${SM_DIR}/target/wasm32-unknown-unknown/release/fluvio_postgres_map.wasm"
}

teardown_smartmodule() {
    kubectl delete smartmodules.fluvio.infinyon.com postgres-map
}

@test "consume with SmartModule" {
    setup_smartmodule

    fluvio consume ${UUID}-topic -B > ./tests/postgres-map.txt --map=postgres-map &
    TASK_PID=$!
    sleep 10
    kill $TASK_PID

    teardown_smartmodule
}
