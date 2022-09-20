use crate::opt::OutputType;
use crate::MqttEvent;
use anyhow::Context;
use serde::Serialize;
use serde_json::Value;

pub(crate) trait Formatter {
    fn to_string(&self, event: &MqttEvent) -> anyhow::Result<String>;
}

pub(crate) fn from_output_type(output_type: &OutputType) -> Box<dyn Formatter> {
    match output_type {
        OutputType::Binary => Box::new(BinaryPayload {}),
        OutputType::Json => Box::new(JsonPayload {}),
    }
}

struct BinaryPayload {}

struct JsonPayload {}

impl Formatter for BinaryPayload {
    fn to_string(&self, event: &MqttEvent) -> anyhow::Result<String> {
        Ok(serde_json::to_string(event)?)
    }
}

impl Formatter for JsonPayload {
    fn to_string(&self, event: &MqttEvent) -> anyhow::Result<String> {
        let payload: Value = serde_json::from_slice(event.payload.as_slice())
            .context("payload is not valid JSON")?;
        #[derive(Serialize)]
        struct Record<'a> {
            mqtt_topic: &'a str,
            payload: Value,
        }
        let record = Record {
            mqtt_topic: event.mqtt_topic.as_str(),
            payload,
        };
        Ok(serde_json::to_string(&record)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_binary_payload_formatting() -> anyhow::Result<()> {
        //given
        let formatter = from_output_type(&OutputType::Binary);
        let event = MqttEvent {
            mqtt_topic: "topic".to_string(),
            payload: b"hello world".to_vec(),
        };

        //when
        let result = formatter.to_string(&event)?;

        //then
        let deserialized: MqttEvent = serde_json::from_str(&result)?;
        assert_eq!(deserialized.mqtt_topic, "topic");
        assert_eq!(deserialized.payload, b"hello world");
        Ok(())
    }

    #[test]
    fn test_json_payload_formatting() -> anyhow::Result<()> {
        //given
        let formatter = from_output_type(&OutputType::Json);
        let event = MqttEvent {
            mqtt_topic: "topic".to_string(),
            payload: b"{\"key\":\"value\"}".to_vec(),
        };

        //when
        let result = formatter.to_string(&event)?;

        //then
        let expected_json = json!({
            "mqtt_topic": "topic",
            "payload": {
                "key": "value"
            }

        });
        assert_eq!(result, serde_json::to_string(&expected_json)?);
        Ok(())
    }

    #[test]
    fn test_json_payload_formatting_invalid_json() {
        //given
        let formatter = from_output_type(&OutputType::Json);
        let event = MqttEvent {
            mqtt_topic: "topic".to_string(),
            payload: b"not json".to_vec(),
        };

        //when
        let result = formatter.to_string(&event);

        //then
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "payload is not valid JSON");
    }
}
