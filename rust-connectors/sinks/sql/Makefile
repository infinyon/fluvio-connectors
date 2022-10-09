
POSTGRES_SINK_NAME=fluvio-sql-sink
FLUVIO_PG_DATABASE_IP=127.0.0.1
FLUVIO_PG_SINK_ENDPOINT=$(FLUVIO_PG_DATABASE_IP):5433

postgres:
	docker run --name $(POSTGRES_SINK_NAME) -p $(FLUVIO_PG_SINK_ENDPOINT):5432/tcp -e POSTGRES_PASSWORD=fluviopassword -e POSTGRES_USER=fluvio -d postgres

postgres-stop:
	docker stop $(POSTGRES_SINK_NAME)
	docker rm -f $(POSTGRES_SINK_NAME)

postgres-tables:
	#PGPASSWORD=fluviopassword docker exec -it $(POSTGRES_SINK_NAME) psql -U fluvio fluvio -c 'CREATE TABLE topic_message (col1 text);'
	PGPASSWORD=fluviopassword docker exec -it $(POSTGRES_SINK_NAME) psql -U fluvio fluvio -c 'CREATE TABLE messages (message text, id int);'

run-dev:
	RUST_BACKTRACE=full RUST_LOG=debug cargo run -- --database-url postgres://fluvio:fluviopassword@$(FLUVIO_PG_SINK_ENDPOINT) --fluvio-topic fluvio-sql-sink

test:
	bats ./tests/simple.bats
