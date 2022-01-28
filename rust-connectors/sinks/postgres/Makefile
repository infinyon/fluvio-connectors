test:
	bats ./tests/simple.bats

POSTGRES_NAME=fluvio-postgres-sink
postgres:
	docker run --name $(POSTGRES_NAME) -e POSTGRES_PASSWORD=fluviopassword -e POSTGRES_USER=fluvio -d postgres

FLUVIO_PG_DATABASE_IP=$(shell docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $(POSTGRES_NAME))

run-dev:
	FLUVIO_PG_DATABASE_URL=postgres://fluvio:fluviopassword@$(FLUVIO_PG_DATABASE_IP):5432 cargo run -- --fluvio-topic postgres

test-dev:
	FLUVIO_PG_DATABASE_URL=postgres://fluvio:fluviopassword@$(FLUVIO_PG_DATABASE_IP):5432 RUST_BACKTRACE=full cargo test -- --nocapture

psql:
	docker exec -it $(POSTGRES_NAME) psql -U fluvio

.EXPORT_ALL_VARIABLES:
FLUVIO_PG_PUBLICATION=fluvio
FLUVIO_PG_SLOT=fluvio
FLUVIO_PG_TOPIC=postgres
