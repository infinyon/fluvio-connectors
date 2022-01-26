# `fluvio-connect-postgres`

A Fluvio connector for reading CDC events from a Postgres database.

## Getting Started (via Docker)

To get started running the Postgres connector in Docker, we first want to
set up a Postgres database in Docker. Run the following commands to quickly
set one up:

```bash
$ docker network create postgres-net
$ docker run -d --name postgres-leader --net postgres-net -p5432:5432 -v "$PWD/postgres.conf":/etc/postgresql/postgresql.conf -e POSTGRES_PASSWORD=mysecretpassword postgres -c 'config_file=/etc/postgresql/postgresql.conf'
```

This creates a docker network called `postgres-net`, which will allow the connector
to communicate with the database. It also starts a Postgres image with the superuser
password `mysecretpassword` and uses the `./postgres.conf` configuration file to enable
logical replication. Note that these commands must be run from this directory.

### Database configuration

We need to run some one-time commands on Postgres itself in order to use the connector.
Use the `psql` command to connect to the database using the commands below, then use
the SQL commands shown to perform the setup.

```bash
$ psql -h localhost -U postgres
Password for user postgres: mysecretpassword
psql (14.0)
Type "help" for help.

postgres=# SELECT pg_create_logical_replication_slot('fluvio', 'pgoutput');
 pg_create_logical_replication_slot
------------------------------------
 (fluvio,0/16FABF0)
(1 row)

postgres=# CREATE PUBLICATION fluvio FOR ALL TABLES;
CREATE PUBLICATION
```

### Launch connector

We need to create a topic for the connector to put the data in:

```bash
$ fluvio topic create postgres
```

Finally, to launch the Postgres connector, invoke it via the `infinyon/fluvio-connect-postgres`
docker image using the following command:

```bash
$ docker run -d --name fluvio-connect-postgres --net postgres-net -v "$HOME/.fluvio/config:/home/fluvio/.fluvio/config" infinyon/fluvio-connect-postgres -- --url=postgres://postgres:mysecretpassword@postgres-leader:5432 --publication=fluvio --slot=fluvio --topic=postgres --fluvio-topic=postgres
```

To see the logs from the connector, use this command:

```bash
$ docker logs -f fluvio-connect-postgres
```
