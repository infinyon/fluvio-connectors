# Fluvio http Connector

## http Source Connector

Sources HTTP Responses given input HTTP request configuration options and `interval` x.

## http Metadata Modifiers

### Connector Configuration

Controls the connector

| Connector Type | Option   | default  | type  | description |
| :---           | :---     | :---     | :---  | :----       |
| Source         | interval | 300      | u64   | Interval between each HTTP Request |

### HTTP Request Configuration

Controls how the each HTTP Request is made

| Option   | default  | type   | description |
| :---     | :---     | :---   | :----       |
| method   | GET      | String | GET, POST, PUT, HEAD |
| endpoint | -        | String | HTTP URL endpoint |
| header|s | -        | String | Request headers Key=Value pairs |
| body     | -        | String | Request body e.g. in POST |

### Record Output Configuration

Controls how the output Record is produced

| Option       | default  | type   | description |
| :---         | :---     | :---   | :----       |
| output_type  | text     | String | text = UTF-8 String Output, json = UTF-8 JSON Serialized String |
| output_parts | body     | String | body = body only, full = all status, header and body parts |

## Record Type Output

| Matrix                                                      | Output                                  |
| :----                                                       | :---                                    |
| output_type = text (default), output_parts = body (default) | Only the body of the HTTP Response      |
| output_type = text (default), output_parts = full           | The full HTTP Response                  |
| output_type = json, output_parts = body (default)           | Only the "body" in JSON struct          |
| output_type = json, output_parts = full                     | HTTP "status", "body" and "header" JSON |