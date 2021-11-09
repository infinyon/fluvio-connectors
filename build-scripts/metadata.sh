#!/usr/bin/env bash
set -e

METADATA_FILE=test.json
echo '[' > ${METADATA_FILE}
for connector in mqtt test-connector;
do
    cargo run --bin ${connector} -- metadata >> ${METADATA_FILE}
done

echo ']' >> ${METADATA_FILE}
