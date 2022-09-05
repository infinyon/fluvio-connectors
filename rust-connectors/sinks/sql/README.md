# Fluvio SQL Sink connector
The SQL Sink connector reads records from Fluvio topic, applies configured transformations, and 
sends new records to the SQL database.
## Supported databases
1. PostgreSQL
2. SQLite

### Data types
| Model           | PostgreSQL                   | SQLite       |                                          
|:----------------|:-----------------------------|:-------------|
| Bool            | BOOL                         | BOOLEAN      |
| Char            | CHAR                         | INTEGER      |
| SmallInt        | SMALLINT, SMALLSERIAL, INT2  | INTEGER      |
| Int             | INT, SERIAL, INT4            | INTEGER      |
| BigInt          | BIGINT, BIGSERIAL, INT8      | BIGINT, INT8 |
| Float           | REAL, FLOAT4                 | REAL         |
| DoublePrecision | DOUBLE PRECISION, FLOAT8     | REAL         |
| Text            | VARCHAR, CHAR(N), TEXT, NAME | TEXT         |
| Bytes           | BYTEA                        | BLOB         |
| Numeric         | NUMERIC                      | REAL         |
| Timestamp       | TIMESTAMP                    | DATETIME     |
| Date            | DATE                         | DATE         |
| Time            | TIME                         | TIME         |
| Uuid            | UUID                         | BLOB, TEXT   |
| Json            | JSON, JSONB                  | TEXT         |


## Transformations
The SQL Sink connector expects the data in [Fluvio SQL Model](../../models/fluvio-model-sql) in JSON format.
In order to work with different data formats or data structures, transformations can be applied.
The transformation is a SmartModule pulled from the SmartModule Hub. Transformations are chained according to the order
in the config. If a SmartModule requires configuration, it is passed via `with` section of `transforms` entry. 
## Usage

### Configuration via parameters
| Option       | default                     | type   | description                                           |
|:-------------|:----------------------------| :---   |:------------------------------------------------------|
| hub_url      | http://127.0.0.1:8080       | String | The paramaters key for the smartmodule hub url        |
| database_url | -                           | String | The paramaters key for the sql database conection url |


### Configuration via secrets
| Option              | default                     | type   | description                                           |
|:--------------------|:----------------------------| :---   |:------------------------------------------------------|
| FLUVIO_HUB_URL      | http://127.0.0.1:8080       | String | The secret key for the smartmodule hub url        |
| FLUVIO_DATABASE_URL | -                           | String | The secret key for the sql database conection url |

### Example
Let's look at the example of the connector with one transformation named `infinyon/json-sql`. The transformation takes
records in JSON format and creates SQL insert operation to `topic_message` table. The value from `device.device_id`
JSON field will be put to `device_id` column and the entire json body to`record` column.

Create a connector configuration file:
```yaml
# connector-config.yaml
name: json-sql-connector
type: sql-sink
version: latest
topic: json-test
create_topic: true
parameters:
  hub-url: 'http://10.96.19.189:8080'
  database-url: 'postgresql://admin:test123@10.96.241.75:5432/postgresdb'
secrets:
  FLUVIO_HUB_URL: 'http://10.96.19.189:8080'
  FLUVIO_DATABASE_URL: 'postgresql://admin:test123@10.96.241.75:5432/postgresdb'
transforms:
  - uses: infinyon/json-sql
    invoke: insert
    with:
      table: "topic_message"
      map-columns:
        "device_id":
          json-key: "device.device_id"
          value:
            type: "int"
            default: "0"
            required: true
        "record":
          json-key: "$"
          value:
            type: "jsonb"
            required: true
```

Create locally managed connector (k8s):
```bash
cargo r --bin connector-run -- apply --config connector-config.yaml
```
or for run it in the Docker:
```bash
cargo r --bin connector-run -- local --config connector-config.yaml
```

To delete the connector run:
```bash
cargo r --bin connector-run -- delete --config connector-config.yaml
```