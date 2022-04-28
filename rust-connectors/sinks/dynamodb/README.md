# Fluvio Dynamodb Connector

## Dynamodb Sink Connector

Controls the Source connector

| Option        | default  | type                        | description                                        |
| :---          | :---     | :---                        | :----                                              |
| table-name    | -        | String                      | The dynamodb table name                            |
| column-names  | -        | comma separated strings     | The column names for the table                     |
| column-types  | -        | comma separated strings     | The column types for the table                     |
| aws-endpoint  | -        | http://localhost:8000 style | An optional command to point to a local dynamodb   |


The first entry in the column list will be the partition key.

### Secret keys
| Option               | default  | type                                              | description                    |
| :---                 | :---     | :---                                              | :----                          |
| table-name           | -        | String                                            | The dynamodb table name        |
| AWS_REGION           | -        | String                                            | The AWS region                 |
| AWS_ACCESS_KEY_ID    | -        | String                                            | Your AWS secret key id         |
| AWS_SECRET_ACCESS_KEY| -        | String                                            | your aws secret access key     |



## Development

`make dynamodb-docker` will use the [amazon
dynamodb-local](https://hub.docker.com/r/amazon/dynamodb-local) image and start
it on `http://localhost:8000`.
