# Fluvio SQL Sink connector
The SQL Sink connector reads records from Fluvio topic, applies configured transformations, and 
sends new records to the SQL database (via `INSERT` statements). 
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
The SQL Sink connector expects the data in [Fluvio SQL Model](../../models/fluvio-model-sql/README.md) in JSON format.
In order to work with different data formats or data structures, transformations can be applied.
The transformation is a SmartModule pulled from the SmartModule Hub. Transformations are chained according to the order
in the config. If a SmartModule requires configuration, it is passed via `with` section of `transforms` entry. 
## Usage

### Configuration via parameters
| Option       | default | type   | description                                           |
|:-------------|:--------| :---   |:------------------------------------------------------|
| hub_url      | -       | String | The parameters key for the smartmodule hub url        |
| database_url | -       | String | The parameters key for the sql database conection url |


### Configuration via secrets
| Option              | default | type   | description                                           |
|:--------------------|:--------| :---   |:------------------------------------------------------|
| FLUVIO_HUB_URL      | -       | String | The secret key for the smartmodule hub url        |
| FLUVIO_DATABASE_URL | -       | String | The secret key for the sql database conection url |

### Example
Let's look at the example of the connector with one transformation named [infinyon/json-sql](../../../smartmodules/json-sql/README.md). The transformation takes
records in JSON format and creates SQL insert operation to `topic_message` table. The value from `device.device_id`
JSON field will be put to `device_id` column and the entire json body to `record` column.

The JSON record:
```json
{
  "device": {
    "device_id": 1
  }
}
```

The SQL database (Postgres):
```
CREATE TABLE topic_message (device_id int, record json);
```

Connector configuration file:
```yaml
# connector-config.yaml
name: json-sql-connector
type: sql-sink
version: latest
topic: json-test
create_topic: true
parameters:
  hub-url: 'HUB_URL'
  database-url: 'postgresql://USERNAME:PASSWORD@HOST:PORT/DB_NAME'
secrets:
  FLUVIO_HUB_URL: 'HUB_URL'
  FLUVIO_DATABASE_URL: 'postgresql://USERNAME:PASSWORD@HOST:PORT/DB_NAME'
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

If you run Fluvio cluster in your own k8s cluster, you can create the connector inside that cluster using the following tool:
```bash
cargo r --bin connector-run -- apply --config connector-config.yaml
```
or if you want to create it in your Docker environment:
```bash
cargo r --bin connector-run -- local --config connector-config.yaml
```

To delete the connector from your k8s cluster run:
```bash
cargo r --bin connector-run -- delete --config connector-config.yaml
```
