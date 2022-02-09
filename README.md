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

| Connector | crates.io   | CI     | Types               | Protocols/Description |
| :---      | :----:      | :----  | :----:              | :---                  |
| http      | TBD         | TBD    | Source              | HTTP 1.0, 1.1, 2.0    |
| mqtt      | TBD         | TBD    | Source              | MQTT V4, V5           |
| postgres  | TBD         | TBD    | Source, Sink        | Postgres CDC          |
| syslog    | TBD         | TBD    | Source, Sink        | Syslog                |

## Libs (rust-connectors/)

| Lib                         | path   | CI   | Type | Description                  |
| :---                        | :---   | :--- | :--- | :---                         |
| [fluvio-connectors-common]  | common | TBD  | Rust | Common Metadata opts         |

## Models (rust-connectors/models)

| Model                     | Language(s) | CI   | Description             |
| :---                      | :---        | :--- | :---                    |
| [fluvio-model-postgres]   | Rust        | TBD  | Postgres Data Model     |

## Examples (examples/)

| Example                   | Language(s) | CI   | Types  | Protocols/Description   |
| :---                      | :---        | :--- | :----: | :---                    |
| [python-client-connector] | Python      | TBD  | Source | Source random cat facts |

## Mocks (rust-connectors/utils/mocks)

| Mock                        | CI   | Type | Description                  |
| :---                        | :--- | :--- | :---                         |
| [http-json-mock]            | TBD  | Mock | Mock used by HTTP Bats       |

## Test Helpers (rust-connectors/utils)

| Helper                      | CI   | Type | Description                  |
| :---                        | :--- | :--- | :---                         |
| [bats-helpers]              | TBD  | Test | Bats Helpers                 |

## Contributing

See [CONTRIBUTING.md]

## Release Process

See [RELEASE.md]