test:
	bats ./tests/simple.bats

POSTGRES_SINK_NAME=fluvio-postgres-sink
POSTGRES_SOURCE_NAME=fluvio-postgres-source
FLUVIO_PG_DATABASE_IP=127.0.0.1
FLUVIO_PG_SINK_ENDPOINT=$(FLUVIO_PG_DATABASE_IP):5433
FLUVIO_PG_SOURCE_ENDPOINT=$(FLUVIO_PG_DATABASE_IP):5432

postgres:
	docker run --name $(POSTGRES_SINK_NAME) -p $(FLUVIO_PG_SINK_ENDPOINT):5432/tcp -e POSTGRES_PASSWORD=fluviopassword -e POSTGRES_USER=fluvio -d postgres
	docker run --name $(POSTGRES_SOURCE_NAME) -p $(FLUVIO_PG_SOURCE_ENDPOINT):5432/tcp -e POSTGRES_PASSWORD=fluviopassword -e POSTGRES_USER=fluvio -d postgres -c 'wal_level=logical'

postgres-clean:
	docker rm -f $(POSTGRES_SINK_NAME) $(POSTGRES_SOURCE_NAME)


run-dev:
	FLUVIO_PG_DATABASE_URL=postgres://fluvio:fluviopassword@$(FLUVIO_PG_SINK_ENDPOINT) cargo run -- --fluvio-topic postgres

test-dev:
	RUST_BACKTRACE=full cargo test -j 1 -- --nocapture

psql-sink:
	PGPASSWORD=fluviopassword psql -h $(FLUVIO_PG_DATABASE_IP) -p 5433 -U fluvio

psql-source:
	PGPASSWORD=fluviopassword psql -h $(FLUVIO_PG_DATABASE_IP) -p 5432 -U fluvio

pg_dump-sink:
	@PGPASSWORD=fluviopassword docker exec -it $(POSTGRES_SINK_NAME) pg_dump -U fluvio

pg_dump-source:
	@PGPASSWORD=fluviopassword docker exec -it $(POSTGRES_SOURCE_NAME) pg_dump -U fluvio


watch-tests:
	cargo watch -s 'make postgres-clean ; make postgres && fluvio topic delete postgres ; fluvio topic create postgres && sleep 5 && make test-dev'

.EXPORT_ALL_VARIABLES:
FLUVIO_PG_PUBLICATION=fluvio
RUST_LOG=info
FLUVIO_PG_SOURCE_DATABASE_URL=postgres://fluvio:fluviopassword@$(FLUVIO_PG_SOURCE_ENDPOINT)
FLUVIO_PG_SINK_DATABASE_URL=postgres://fluvio:fluviopassword@$(FLUVIO_PG_SINK_ENDPOINT)
