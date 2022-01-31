test:
	bats ./tests/simple.bats

POSTGRES_NAME=fluvio-postgres-sink
postgres:
	docker run --name $(POSTGRES_NAME) -p 127.0.0.1:5433:5432/tcp -e POSTGRES_PASSWORD=fluviopassword -e POSTGRES_USER=fluvio -d postgres -c 'wal_level=logical'

#FLUVIO_PG_DATABASE_IP=$(shell docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $(POSTGRES_NAME))
FLUVIO_PG_DATABASE_IP=127.0.0.1

run-dev:
	FLUVIO_PG_DATABASE_URL=postgres://fluvio:fluviopassword@$(FLUVIO_PG_DATABASE_IP):5433 cargo run -- --fluvio-topic postgres

test-dev:
	FLUVIO_PG_DATABASE_URL=postgres://fluvio:fluviopassword@$(FLUVIO_PG_DATABASE_IP):5433 RUST_BACKTRACE=full cargo test -- --nocapture

psql:
	PGPASSWORD=fluviopassword psql -h $(FLUVIO_PG_DATABASE_IP) -p 5433 -U fluvio
	#docker exec -it $(POSTGRES_NAME) psql -U fluvio

.EXPORT_ALL_VARIABLES:
FLUVIO_PG_PUBLICATION=fluvio
RUST_LOG=info
