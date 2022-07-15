# Connector Change Log
## http Version 0.3.1 - UNRELEASED
* Added support to smartmodule-parameters and smartmodule without explicit type. ([PR #287](https://github.com/infinyon/fluvio-connectors/pull/287)

## http Version 0.3.0 - 2022-Jul-07
* `interval` field uses human readable values, "1s", "20ms", etc. ([PR #274](https://github.com/infinyon/fluvio-connectors/pull/274))
* `source-linger` field uses human readable values, "1s", "20ms", etc. ([PR #273](https://github.com/infinyon/fluvio-connectors/pull/273))

## http Version 0.2.1 - 2022-June
* Support for `user-agent` argument. ([PR #216](https://github.com/infinyon/fluvio-connectors/pull/216))
* Aggregate smart modules in common options. ([PR #216](https://github.com/infinyon/fluvio-connectors/pull/216))

## http Version 0.2.0 - 2022-Feb-7
* Feature json Response Type Record ([PR #141](https://github.com/infinyon/fluvio-connectors/pull/141))
* Deprecate (Breaking) Metadata `output_format` in favor of `output_parts` [ `body` _(default)_ | `full` ]
* Add Metadata `output_type` [ `text` _(default)_ | `json` ]

## http Version 0.1.1 - 2022-Jan-31
* Feature full Response Parts Record ([PR #127](https://github.com/infinyon/fluvio-connectors/pull/127))
* Add Metadata `output_format` [ `body` _(default)_ | `full` ] _(Renamed to `output_type` in 0.2.0)_

## http Version 0.1.0 - 2021-Nov-9
* Initial version with text/body Response (default) Record
