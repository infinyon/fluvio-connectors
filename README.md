<div align="center">
<h1>Fluvio - Connectors</h1>
<a href="https://fluvio.io" target="_blank">
 <strong>The programmable data streaming platform - Connectors</strong>
 </a>
<br>
<br>

<div>
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

## Rust Connectors (rust-connectors/sources/)

| Connector                 | Release     | Docker | CI     | Types               | Protocols/Description |
| :---                      | :----:      | :---   | :----  | :----:              | :---                  |
| [fluvio-connect-http]     | TBD         | TBD    | TBD    | Source              | HTTP 1.0, 1.1, 2.0    |
| [fluvio-connect-mqtt]     | TBD         | TBD    | TBD    | Source              | MQTT V4, V5           |
| [fluvio-connect-postgres] | TBD         | TBD    | TBD    | Source, Sink        | Postgres CDC          |
| [fluvio-connect-syslog]   | TBD         | TBD    | TBD    | Source, Sink        | Syslog                |

[fluvio-connect-http]: https://github.com/infinyon/fluvio-connectors/tree/main/rust-connectors/sources/http
[fluvio-connect-mqtt]: https://github.com/infinyon/fluvio-connectors/tree/main/rust-connectors/sources/mqtt
[fluvio-connect-postgres]: https://github.com/infinyon/fluvio-connectors/tree/main/rust-connectors/sources/postgres
[fluvio-connect-syslog]: https://github.com/infinyon/fluvio-connectors/tree/main/rust-connectors/sources/syslog

## Libs (rust-connectors/)

| Lib                         | path   | CI   | Type | Description                  |
| :---                        | :---   | :--- | :--- | :---                         |
| [fluvio-connectors-common]  | common | TBD  | Rust | Common Metadata opts         |

[fluvio-connectors-common]: https://github.com/infinyon/fluvio-connectors/tree/main/rust-connectors/common

## Models (rust-connectors/models)

| Model                     | Language(s) | CI   | Description             |
| :---                      | :---        | :--- | :---                    |
| [fluvio-model-postgres]   | Rust        | TBD  | Postgres Data Model     |

[fluvio-model-postgres]: https://github.com/infinyon/fluvio-connectors/tree/main/rust-connectors/models/fluvio-model-postgres

## Examples (examples/)

| Example                   | Language(s) | CI   | Types  | Protocols/Description   |
| :---                      | :---        | :--- | :----: | :---                    |
| [python-client-connector] | Python      | TBD  | Source | Source random cat facts |

[python-client-connector]: https://github.com/infinyon/fluvio-connectors/tree/main/examples/python-client-connector

## Mocks (rust-connectors/utils/mocks)

| Mock                        | CI   | Type | Description                  |
| :---                        | :--- | :--- | :---                         |
| [http-json-mock]            | TBD  | Mock | Mock used by HTTP Bats       |

[http-json-mock]: https://github.com/infinyon/fluvio-connectors/tree/main/rust-connectors/utils/mocks/http-json-mock

## Smart Stream (rust-connectors/utils)

| Smart Stream                | CI   | Description                  |
| :---                        | :--- | :---                         |
| [fluvio-smartstream-map]    | TBD  | Map                          |

[fluvio-smartstream-map]: https://github.com/infinyon/fluvio-connectors/tree/main/rust-connectors/utils/fluvio-smartstream-map

## Other Helpers (rust-connectors/utils)

| Helper                      | CI   | Type | Description                  |
| :---                        | :--- | :--- | :---                         |
| [bats-helpers]              | TBD  | Test | Bats Helpers                 |
| [test-connector]            | TBD  | Test | Test Connector               |
| 

[bats-helpers]: https://github.com/infinyon/fluvio-connectors/tree/main/rust-connectors/utils/bats-helpers
[test-connector]: https://github.com/infinyon/fluvio-connectors/tree/main/rust-connectors/utils/test-connector

## Contributing

See [CONTRIBUTING.md]

## Release Process

See [RELEASE.md]