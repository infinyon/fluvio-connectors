#!/usr/bin/env bats

setup_postgres() {
    kubectl apply -f ./postgres.yml
    sleep 60

    PGPASSWORD=mysecretpassword psql -h "$(minikube ip)" -U postgres -p5432 -c "SELECT pg_create_logical_replication_slot('fluvio', 'pgoutput');" || true
    PGPASSWORD=mysecretpassword psql -h "$(minikube ip)" -U postgres -p5432 -c "CREATE PUBLICATION fluvio FOR ALL TABLES;" || true
}

setup() {
    setup_postgres

    FILE="$(mktemp -d)/connect.yml"
    echo "Current directory: $(pwd)" >&2
    cp ./tests/connect.yml $FILE
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
}

@test "consume connector" {
    fluvio consume ${UUID}-topic -B &
    TASK_PID=$!
    sleep 60
    kill $TASK_PID
}
