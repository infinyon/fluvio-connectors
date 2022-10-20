# Fluvio mqtt Connector

Latest release version = "0.3.0"

## mqtt Source Connector

Sources mqtt events given input mqtt consumer configuration options.

## Protocol Support

MQTT V3.1.1 and V5

## mqtt Metadata Modifiers

### Source Configuration

#### Parameters

| Option              | default  | type     | description                                                                                                                                          |
|:--------------------|:---------|:---------|:-----------------------------------------------------------------------------------------------------------------------------------------------------|
| timeout             | 60       | u64      | mqtt broker connect timeout in seconds                                                                                                               |
| mqtt_url            | -        | String   | mqtt_url MQTT url which includes schema, domain and port. *USE MQTT_URL* in secrets if you need to supply credentials such as username and password. |
| mqtt_topic          | -        | String   | mqtt topic to subscribe and source events from                                                                                                       |
| client_id           | UUID V4  | String   | mqtt client ID                                                                                                                                       |
| payload_output_type | binary   | String   | controls how the output of `payload` field is produced                                                                                               |

#### Secrets

| Option        | default  | type   | description                                             |
| :---          | :---     | :---   | :----                                                   |
| MQTT_URL      | -        | String | MQTT_URL MQTT url which in addition includes username and password   |

### Record Output Configuration

None - Record output is UTF-8 JSON Serialized String.

### Record Type Output

| Matrix  | Output                                                        |
| :---    |:--------------------------------------------------------------|
| default | JSON Serialized string with fields `mqtt_topic` and `payload` |

### Payload Output Configuration

Controls how the output of `payload` field is produced

| Option              | default | type     | 
|:--------------------|:--------|:---------|
| payload_output_type | binary  | String   | 


### Payload Output Type

| Value  | Output                       |
|:-------|:-----------------------------|
| binary | Array of bytes               |
| json   | UTF-8 JSON Serialized String |


## Testing

Use following configuration:

```
version: latest
name: my-mqtt-new
type: mqtt-source
topic: mqtt-topic
direction: source
create-topic: true
parameters:
  mqtt_topic: "ag-mqtt-topic"
  payload_output_type: json  
secrets:
  MQTT_URL: mqtt://test.mosquitto.org/
```


Install MQTT Client such as
```
# for mac , this takes while....
brew install mosquitto

mosquitto_pub -h test.mosquitto.org -t ag-mqtt-topic -m '{"device": {"device_id":17, "name":"device17"}}'
```