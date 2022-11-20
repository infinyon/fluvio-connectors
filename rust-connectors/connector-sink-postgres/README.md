# Fluvio Postgres Sink Connector

This is the sink side of the postgres source connector.

## Usage
Using a `postgres-sink.yaml` that looks something like:

```yaml
version: latest
name: postgres-sink-connector
type: postgres-sink
topic: postgres-topic
create_topic: true
secrets:
  FLUVIO_PG_DATABASE_URL: postgres://fluvio:fluviopassword@192.168.1.12:5432
```

### Sink Configuration Parameters

Controls the Sinxk connector

| Option                   | default  | type   | description |
| :---                     | :---     | :---   | :----       |
| FLUVIO_PG_DATABASE_URL   | -        | String | The secret key for the postgres database url|
| url                      | -        | String | The args for the postgres database url|

This will consume events from [`postgres-source`](../../sources/postgres) via
[`fluvio-model-postgres`](../../models/fluvio-model-postgres).

## Notes
* [Postgres logical replication has some
restrictions](https://www.postgresql.org/docs/14/logical-replication-restrictions.html).
If you have an existing postgres server and want to start using fluvio's source
and sink connectors for replication, you will need to do
[`pg_dump`](https://www.postgresql.org/docs/current/app-pgdump.html) and import
it via [`psql`](https://www.postgresql.org/docs/current/app-psql.html)
* To keep track of previous events, the sink connector stores the fluvio offset
in the `fluvio.offset` table.

## Testing
* `make postgres` will create a postgres source and a postgres sink container.
* `make test-dev` will run cargo tests against said docker container.
