# Fluvio syslog Connector

## syslog Source Connector

Sources syslog events given the source configuration.

## syslog Sink Connector

Consumes syslog events given topic.

## syslog Metadata Modifiers

| Option   | default  | type  | description |
| :---     | :---     | :---  | :----       |
| produce  | -        | flag  | Connector is set to act as a Source |
| consume  | -        | flag  | Connector is set to act as a Sink |

## syslog Source Configuration

| Option   | default  | type   | description |
| :---     | :---     | :---   | :----       |
| bind     | -        | String | Bind and listen to this address to source syslog events. Not supported on WASM32 arch |
| file     | -        | String | Tail a local syslog file |
| topic    | -        | String | Target topic to produce the syslog events to |

## syslog Sink Configuration

| Option   | default  | type   | description |
| :---     | :---     | :---   | :----       |
| topic    | -        | String | Source topic to consume the syslog events from |

## Record Source Output Configuration

None - The syslog in Source connector is transmitted as UTF-8 (lossy) String as-is read (file) / received (bind)

## Record Type Output

| Matrix                                                      | Output                                  |
| :----                                                       | :---                                    |
| default                                                     | Syslog message as-is UTF-8 (lossy) String |

