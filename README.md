<div align="center">
<h1>Fluvio - Connectors</h1>
<a href="https://fluvio.io" target="_blank">
 <strong>The programmable data streaming platform - Connectors</strong>
 </a>
<br>
<br>

<!-- CI Combined status -->
<a href="https://github.com/infinyon/fluvio-connectors/actions/workflows/ci.yml">
<img src="https://github.com/infinyon/fluvio-connectors/workflows/CI/badge.svg" alt="CI Status" />
</a>

<!-- CD status
<a href="https://github.com/infinyon/fluvio-connectors/actions/workflows/cd_dev.yaml">
<img src="https://github.com/infinyon/fluvio-connectors/workflows/CD_Dev/badge.svg" alt="CD Status" />
</a> -->

<a href="https://discordapp.com/invite/bBG2dTz">
<img src="https://img.shields.io/discord/695712741381636168.svg?logo=discord&style=flat" alt="chat" />
</a>
</div>

## Rust Source Connectors (rust-connectors/sources/)

| Connector          | Release     | Docker (infinyon/connect-)      | Protocols/Description |
| :---               | :----:      | :---                            | :---                  |
| [sources/http]     | 0.2.0       | [infinyon/http]                 | HTTP 1.0, 1.1, 2.0    |
| [sources/mqtt]     | 0.1.1       | [infinyon/mqtt]                 | MQTT V4, V5           |
| [sources/postgres] | 0.1.0       | [infinyon/postgres-source]      | Postgres CDC          |
| [sources/syslog]   | 0.1.0       | [infinyon/syslog]               | Syslog                |

[sources/http]: https://github.com/infinyon/fluvio-connectors/tree/main/rust-connectors/sources/http
[sources/mqtt]: https://github.com/infinyon/fluvio-connectors/tree/main/rust-connectors/sources/mqtt
[sources/postgres]: https://github.com/infinyon/fluvio-connectors/tree/main/rust-connectors/sources/postgres
[sources/syslog]: https://github.com/infinyon/fluvio-connectors/tree/main/rust-connectors/sources/syslog

[infinyon/http]: https://hub.docker.com/r/infinyon/fluvio-connect-http
[infinyon/mqtt]: https://hub.docker.com/r/infinyon/fluvio-connect-mqtt
[infinyon/postgres-source]: https://hub.docker.com/r/infinyon/fluvio-connect-postgres-source
[infinyon/syslog]: https://hub.docker.com/r/infinyon/fluvio-connect-syslog

## Rust Sink Connectors (rust-connectors/sinks/)

| Connector          | Release     | Docker (infinyon/connect-)      | Protocols/Description |
| :---               | :----:      | :---                            | :---                  |
| [sinks/postgres]   | 0.1.0       | [infinyon/postgres-sink]        | Postgres CDC          |

[sinks/postgres]: https://github.com/infinyon/fluvio-connectors/tree/main/rust-connectors/sinks/postgres
[infinyon/postgres-sink]: https://hub.docker.com/r/infinyon/fluvio-connect-postgres-sink

## Libs (rust-connectors/)

| Lib                         | path   | Type | Description                  |
| :---                        | :---   | :--- | :---                         |
| [fluvio-connectors-common]  | common | Rust | Common Metadata opts         |

[fluvio-connectors-common]: https://github.com/infinyon/fluvio-connectors/tree/main/rust-connectors/common

## Models (rust-connectors/models)

| Model                     | Language(s) | Description             |
| :---                      | :---        | :---                    |
| [fluvio-model-postgres]   | Rust        | Postgres Data Model     |

[fluvio-model-postgres]: https://github.com/infinyon/fluvio-connectors/tree/main/rust-connectors/models/fluvio-model-postgres

## Examples (examples/)

| Example                   | Language(s) | Types  | Protocols/Description   |
| :---                      | :---        | :----: | :---                    |
| [python-client-connector] | Python      | Source | Source random cat facts |

[python-client-connector]: https://github.com/infinyon/fluvio-connectors/tree/main/examples/python-client-connector

## Mocks (rust-connectors/utils/mocks)

| Mock                        | Type | Description                  |
| :---                        | :--- | :---                         |
| [http-json-mock]            | Mock | Mock used by HTTP Bats       |

[http-json-mock]: https://github.com/infinyon/fluvio-connectors/tree/main/rust-connectors/utils/mocks/http-json-mock

## Smart Stream (rust-connectors/utils)

| Smart Stream                | Description                  |
| :---                        | :---                         |
| [fluvio-smartstream-map]    | Map                          |

[fluvio-smartstream-map]: https://github.com/infinyon/fluvio-connectors/tree/main/rust-connectors/utils/fluvio-smartstream-map

## Other Helpers (rust-connectors/utils)

| Helper                      | Type | Description                  |
| :---                        | :--- | :---                         |
| [bats-helpers]              | Test | Bats Helpers                 |
| [test-connector]            | Test | Test Connector               | 

[bats-helpers]: https://github.com/infinyon/fluvio-connectors/tree/main/rust-connectors/utils/bats-helpers
[test-connector]: https://github.com/infinyon/fluvio-connectors/tree/main/rust-connectors/utils/test-connector

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)

