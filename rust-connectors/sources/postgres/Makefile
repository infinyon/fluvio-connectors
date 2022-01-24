test:
	bats ./tests/simple.bats

postgres-docker:
	docker run --name fluvio-postgres -e POSTGRES_PASSWORD=fluviopassword -e POSTGRES_USER=fluvio -d postgres -c 'wal_level=logical'

FLUVIO_PG_DATABASE_IP=$(shell docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' fluvio-postgres)

run-dev:
	FLUVIO_PG_DATABASE_URL=postgres://fluvio:fluviopassword@$(FLUVIO_PG_DATABASE_IP):5432 cargo run -- --fluvio-topic postgres

test-dev:
	FLUVIO_PG_DATABASE_URL=postgres://fluvio:fluviopassword@$(FLUVIO_PG_DATABASE_IP):5432 cargo test -- --nocapture

psql:
	docker exec -it fluvio-postgres psql -U fluvio

.EXPORT_ALL_VARIABLES:
FLUVIO_PG_PUBLICATION=fluvio
FLUVIO_PG_SLOT=fluvio
FLUVIO_PG_TOPIC=postgres
