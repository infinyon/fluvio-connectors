# Github Workflow

Conventional commits are preferred for the commit messages:
https://www.conventionalcommits.org/en/

The below should be run in the development environment:

- cargo fmt
- cargo test
- make test  _integration tests_

Bors is used with the squash-merge functionality to keep the combined commit log clean.

This means that each originating PR is closed and it's commits are combined into a separate PR.

This will not affect contribution statistics as the new bors PR is owned by the original contributor.

The originating PR will be closed with the informational prefix [Merged by Bors] 

# Adding a new connector

A given connector must have a `metadata` subcommand. This subcommand will
return a json object containing a `name`, `direction` (which is a string of `source` or `sink`), `version`, `description`
and a `schema` where the `schema` object is a
[draft-07](http://json-schema.org/draft-07/schema#) schema which describes the
commandline arguments for the connector.

Example:
```json
{
  "name": "mqtt",
  "direction": "Source",
  "schema": {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "MqttOpts",
    "type": "object",
    "required": [
      "fluvio_topic",
      "mqtt_topic",
      "mqtt_url"
    ],
    "properties": {
      "fluvio_topic": {
        "type": "string"
      },
      "mqtt_topic": {
        "type": "string"
      },
      "mqtt_url": {
        "type": "string"
      },
      "qos": {
        "type": [
          "integer",
          "null"
        ],
        "format": "int32"
      },
      "timeout": {
        "type": [
          "integer",
          "null"
        ],
        "format": "uint64",
        "minimum": 0
      }
    }
  },
  "version": "0.1.0",
  "description": ""
}
```
