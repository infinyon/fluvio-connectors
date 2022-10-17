# JSON to SQL transformation SmartModule
This is a `map` type SmartModule that converts records in arbitrary JSON into
records in [SQL model](../../rust-connectors/models/fluvio-model-sql). The transformation specification is defined in 
JSON format and passed to the SmartModule via constructor call (`mapping` parameter in `SmartModuleExtraParams`).

## The specification
The specification consists of mapping that defines the JSON path of the input record - where to read an entry
and the output corresponding SQL operation. For example:
```json
{
  "table" : "target_table",
  "map-columns": {
    "target_column_name" : {
      "json-key": "device.number",
      "value": {
        "type": "int4"
      }
    }
  }
}
```
This is the simplest mapping where you read only one value from the input record and create SQL
insert operation to table `target_table` and column `target_column_name`. So, if you have an input record:

```json
{
  "device": {
    "number": 1
  }
}
```
you will get SQL insert operation. The text equivalent of such an operation
would look like 
```
INSERT INTO target_table (target_column_name) values (1)
```
### Default and Required
It is possible to specify if the input field is required by setting `required: true`. If it is not present,
the processing will result in an error.

The default value is set by `default: 0`. If the value is not present, the default one will be taken.

We can modify the previous mapping example to:
```json
{
  "table" : "target_table",
  "map-columns": {
    "target_column_name" : {
      "json-key": "device.number",
      "value": {
        "type": "int4"
      }
    },
    "another_column_name" : {
      "json-key": "device.id",
      "value": {
        "type": "int4",
        "default": 0,
        "required": false
      }
    }
  }
}
```
and we will get
```
INSERT INTO target_table (target_column_name, another_column_name) values (1,0)
```

### Accessing JSON
`json-key` in the mapping is a path to the field inside JSON object.
For the following JSON record:

```json
{
  "device": {
    "number": 1
  },
  "metrics": ["one", "two", "three"]
}
```
we will have the following correspondence between `json-key` and result:

| json-key         | returning result                                             |
|------------------|--------------------------------------------------------------|
| device.number    | 1                                                            |
| $.device.number  | 1                                                            |
| .device.number   | 1                                                            |
| device.metrics   | ["one", "two", "three"]                                      |
| device.metrics.0 | "one"                                                        |
| device.metrics.1 | "two"                                                        |
| $                | {"device":{"number": 1}, "metrics": ["one", "two", "three"]} |

### Data types
The list of supported types and corresponding types from [SQL model](../../rust-connectors/models/fluvio-model-sql):

| Mapping                                     | Model           |
|---------------------------------------------|-----------------|
| int8, bigint                                | BigInt          |
| int4, int, integer                          | Int             |
| int2, smallint                              | SmallInt        |
| bool, boolean                               | Bool            |
| bytes, bytea                                | Bytes           |
| text                                        | Text            |
| float4, float, real                         | Float           |
| float8, "double precision", doubleprecision | DoublePrecision |
| decimal, numeric                            | Numeric         |
| date                                        | Date            |
| time                                        | Time            |
| timestamp                                   | Timestamp       |
| json, jsonb                                 | Json            |
| uuid                                        | Uuid            |

