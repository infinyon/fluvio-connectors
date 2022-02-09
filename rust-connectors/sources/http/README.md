# Fluvio http Connector

## http Source Connector

Sources HTTP Responses given input HTTP request configuration options and `interval` x.

## Protocol Support

* HTTP/1.0, HTTP/1.1, HTTP/2.0

## http Metadata Modifiers

### Source Configuration

Controls the Source connector

| Option   | default  | type  | description |
| :---     | :---     | :---  | :----       |
| interval | 300      | u64   | Interval between each HTTP Request |

### HTTP Request Configuration

Controls how each HTTP Request is made

| Option   | default  | type   | description |
| :---     | :---     | :---   | :----       |
| method   | GET      | String | GET, POST, PUT, HEAD |
| endpoint | -        | String | HTTP URL endpoint |
| headers  | -        | String | Request header(s) Key=Value pairs |
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