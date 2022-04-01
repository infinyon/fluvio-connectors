# Fluvio Dynamodb Connector

## Dynamodb Sink Connector

Controls the Source connector

| Option        | default  | type                                              | description                                 |
| :---          | :---     | :---                                              | :----                                       |
| table-name    | -        | String                                            | The dynamodb table name                     |
| column-names  | -        | comma separated strings                           | The column names for the table              |
| column-types  | -        | comma separated strings                           | The column types for the table              |
| aws-endpoint  | -        | An optional command to point to a local dynamodb  | The column types for the table              |


## Development

`make dynamodb-docker` will use the [amazon
dynamodb-local](https://hub.docker.com/r/amazon/dynamodb-local) image and start
it on `http://localhost:8000`.
