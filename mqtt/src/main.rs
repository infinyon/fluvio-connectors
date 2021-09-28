use paho_mqtt::client::Client as MqttClient;
//use paho_mqtt::AsyncClient as MqttClient;
use paho_mqtt::CreateOptions;
mod error;
use error::MqttConnectorError;

fn try_reconnect(cli: &MqttClient) -> bool {
    println!("Connection lost. Waiting to retry connection");
    for _ in 0..12 {
        // TODO: Make this back of exponentially
        std::thread::sleep(std::time::Duration::from_millis(5000));
        if cli.reconnect().is_ok() {
            println!("Successfully reconnected");
            return true;
        }
    }
    println!("Unable to reconnect after several attempts.");
    false
}

#[async_std::main]
async fn main() -> Result<(), MqttConnectorError> {
    fluvio_future::subscriber::init_tracer(None);
    let mqtt_url = "mqtt.hsl.fi";
    let mqtt_topic = "/hfp/v2/journey/#";
    let mqtt_qos = 0;
    let mqtt_timeout_seconds = 60;
    let fluvio_topic = "mqtt";

    let timeout = std::time::Duration::from_secs(mqtt_timeout_seconds);
    let mut mqtt_client = MqttClient::new(CreateOptions::from(mqtt_url))?;
    mqtt_client.set_timeout(timeout);
    let rx = mqtt_client.start_consuming();
    mqtt_client.connect(None)?;
    mqtt_client.subscribe(mqtt_topic, mqtt_qos)?;

    let fluvio_client = fluvio::Fluvio::connect().await?;
    let producer = fluvio_client.topic_producer(fluvio_topic).await?;

    for msg in rx.iter() {
        if let Some(msg) = msg {
            let mqtt_topic = msg.topic();
            let mqtt_payload = msg.payload();
            let _ = producer.send(mqtt_topic, mqtt_payload).await?;
        } else if mqtt_client.is_connected() || !try_reconnect(&mqtt_client) {
            break;
        }
    }

    println!("Hello, world!");
    Ok(())
}
