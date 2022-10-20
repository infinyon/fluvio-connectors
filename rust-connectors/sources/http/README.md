# Fluvio http Connector

Latest release version = "0.3.0"

## http Source Connector

Sources HTTP Responses given input HTTP request configuration options and `interval` x.

## Protocol Support

* HTTP/1.0, HTTP/1.1, HTTP/2.0

## http Metadata Modifiers

### Source Configuration

Controls the Source connector

| Option   | default | type   | description                                                                                |
| :------- | :------ | :----- | :----------------------------------------------------------------------------------------- |
| interval | 10s     | String | Interval between each HTTP Request. This is in the form of "1s", "10ms", "1m", "1ns", etc. |

### HTTP Request Configuration

Controls how each HTTP Request is made

| Option     | default                    | type   | description                       |
| :--------- | :------------------------- | :----- | :-------------------------------- |
| method     | GET                        | String | GET, POST, PUT, HEAD              |
| endpoint   | -                          | String | HTTP URL endpoint                 |
| headers    | -                          | String | Request header(s) Key=Value pairs |
| body       | -                          | String | Request body e.g. in POST         |
| user-agent | "fluvio/http-source 0.1.0" | String | Request user-agent                |

### Record Output Configuration

Controls how the output Record is produced

| Option       | default | type   | description                                                     |
| :----------- | :------ | :----- | :-------------------------------------------------------------- |
| output_type  | text    | String | text = UTF-8 String Output, json = UTF-8 JSON Serialized String |
| output_parts | body    | String | body = body only, full = all status, header and body parts      |

## Record Type Output

| Matrix                                                      | Output                                  |
| :---------------------------------------------------------- | :-------------------------------------- |
| output_type = text (default), output_parts = body (default) | Only the body of the HTTP Response      |
| output_type = text (default), output_parts = full           | The full HTTP Response                  |
| output_type = json, output_parts = body (default)           | Only the "body" in JSON struct          |
| output_type = json, output_parts = full                     | HTTP "status", "body" and "header" JSON |

## Example Config

```
version: latest
name: cat-facts
type: http-source
topic: cat-facts
direction: source
parameters:
  endpoint: https://catfact.ninja/fact
  interval: 10s
```