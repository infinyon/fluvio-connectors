# Fluvio mqtt Connector

## mqtt Source Connector

Sources mqtt events given input mqtt consumer configuration options.

## Protocol Support

MQTT V3.1.1 and V5

## mqtt Metadata Modifiers

### Source Configuration

#### Paramaters

| Option        | default  | type   | description                                             |
| :---          | :---     | :---   | :----                                                   |
| timeout       | 60       | u64    | mqtt broker connect timeout in seconds                  |
| mqtt_url      | -        | String | mqtt_url MQTT url which includes schema, domain and port. *USE MQTT_URL* in secrets if you need to supply credentials such as username and password.|
| mqtt_topic    | -        | String | mqtt topic to subscribe and source events from          |
| client_id     | UUID V4  | String | mqtt client ID                                          |

#### Secrets

| Option        | default  | type   | description                                             |
| :---          | :---     | :---   | :----                                                   |
| MQTT_URL      | -        | String | MQTT_URL MQTT url which in addition includes username and password   |

### Record Output Configuration

None - Record output is UTF-8 JSON Serialized String.

### Record Type Output

| Matrix  | Output |
| :---    | :---   |
| default | JSON Serialized string with fields `mqtt_topic` and `payload` holding array of bytes |
