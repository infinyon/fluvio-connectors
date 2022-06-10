# Fluvio Kafka Sink Connector

This is a connector for taking data from a Kafka topic and going to a fluvio topic


## Controls the Sink connector

| Option               | default               | type     | description                            |
| :---                 | :---                  | :---     | :----                                  |
| kafka-url            | -                     | String   | The url for the kafka connector        |
| kafka-topic          | same topic as fluvio  | String   | The kafka topic                        |
| kafka-partition      | 0                     | String   | The kafka partition                    |
