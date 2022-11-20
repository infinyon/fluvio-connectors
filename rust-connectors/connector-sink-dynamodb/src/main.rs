use aws_sdk_dynamodb::{
    model::{
        AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ProvisionedThroughput,
        ScalarAttributeType,
    },
    Client, Endpoint,
};
use clap::Parser;
use fluvio_connectors_common::fluvio::Record;
use fluvio_connectors_common::opt::CommonConnectorOpt;
use fluvio_connectors_common::{common_initialize, git_hash_version};
use fluvio_future::tracing::{error, info};
use schemars::{schema_for, JsonSchema};
use serde_json::value::Value;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    common_initialize!();
    if let Some("metadata") = std::env::args().nth(1).as_deref() {
        let schema = serde_json::json!({
            "name": env!("CARGO_PKG_NAME"),
            "version": env!("CARGO_PKG_VERSION"),
            "description": env!("CARGO_PKG_DESCRIPTION"),
            "direction": "Source",
            "schema": schema_for!(DynamoDbOpt),
        });
        println!("{}", serde_json::to_string_pretty(&schema).unwrap());
        return Ok(());
    }
    let opts: DynamoDbOpt = DynamoDbOpt::from_args();
    opts.common.enable_logging();
    info!(
        connector_version = env!("CARGO_PKG_VERSION"),
        git_hash = git_hash_version(),
        "Starting DynamoDB sink connector",
    );

    opts.execute().await?;
    Ok(())
}

#[derive(Parser, Debug, JsonSchema, Clone)]
pub struct DynamoDbOpt {
    #[clap(long)]
    pub aws_endpoint: Option<String>,

    #[clap(long)]
    pub table_name: String,

    #[clap(long)]
    pub column_names: String,

    #[clap(long)]
    pub column_types: String,

    #[clap(flatten)]
    #[schemars(flatten)]
    pub common: CommonConnectorOpt,
}

impl DynamoDbOpt {
    pub async fn execute(&self) -> anyhow::Result<()> {
        let config = aws_config::load_from_env().await;
        let mut builder = aws_sdk_dynamodb::config::Builder::from(&config);
        if let Some(endpoint) = &self.aws_endpoint {
            let endpoint = Endpoint::mutable(endpoint.parse()?);
            builder = builder.endpoint_resolver(endpoint)
        }

        let dynamodb_local_config = builder.build();

        let client = Client::from_conf(dynamodb_local_config);
        self.create_table(&client).await?;

        let mut stream = self.common.create_consumer_stream("dynamodb").await?;
        info!("Starting stream");
        while let Some(Ok(record)) = stream.next().await {
            if let Err(e) = self.send_to_dynamodb(&record, &client).await {
                error!("{:?}", e);
            }
        }
        Ok(())
    }

    fn serde_value_to_dynamo_value(value: &Value) -> AttributeValue {
        match value {
            Value::String(value) => AttributeValue::S(value.to_string()),
            Value::Number(value) => AttributeValue::N(value.to_string()),
            Value::Bool(value) => AttributeValue::Bool(*value),
            Value::Null => AttributeValue::Null(true),
            Value::Array(value) => {
                if !value.iter().any(|v| !v.is_string()) {
                    let set: Vec<String> = value.iter().map(|v| v.to_string()).collect();
                    AttributeValue::Ss(set)
                } else if !value.iter().any(|v| !v.is_number()) {
                    let set: Vec<String> = value.iter().map(|v| v.to_string()).collect();
                    AttributeValue::Ns(set)
                } else {
                    let list: Vec<AttributeValue> = value
                        .iter()
                        .map(Self::serde_value_to_dynamo_value)
                        .collect();
                    AttributeValue::L(list)
                }
            }
            Value::Object(value) => {
                let map: std::collections::HashMap<String, AttributeValue> = value
                    .iter()
                    .map(|(key, value)| (key.clone(), Self::serde_value_to_dynamo_value(value)))
                    .collect();
                AttributeValue::M(map)
            }
        }
    }

    pub async fn send_to_dynamodb(
        &self,
        record: &Record,
        client: &aws_sdk_dynamodb::Client,
    ) -> anyhow::Result<()> {
        let json: Value = serde_json::from_slice(record.value())?;
        let mut request = client.put_item().table_name(&self.table_name);
        let column_names = self.column_names.split(',');

        for column in column_names {
            if let Some(value) = json.get(column) {
                let attr = Self::serde_value_to_dynamo_value(value);
                request = request.item(column, attr);
            }
        }
        info!("dynamodb request {:?}", request);
        let _ = request.send().await?;

        Ok(())
    }

    async fn create_table(&self, client: &aws_sdk_dynamodb::Client) -> anyhow::Result<()> {
        let resp = client.list_tables().send().await?;

        let names = resp.table_names().unwrap_or_default();

        let table_name = self.table_name.clone();
        if names.contains(&table_name) {
            return Ok(());
        }

        let column_names: Vec<&str> = self.column_names.split(',').collect();
        let column_types: Vec<&str> = self.column_types.split(',').collect();

        if column_names.is_empty() {
            panic!("Must have one ore more columns");
        }
        if column_names.len() != column_types.len() {
            panic!("Must have the same number of column names as column types");
        }
        let primary_key = column_names.first().unwrap().to_string();

        if !column_names.contains(&primary_key.as_str()) {
            panic!("Key Schema must be a column");
        }

        let ks = KeySchemaElement::builder()
            .attribute_name(&*primary_key)
            .key_type(KeyType::Hash)
            .build();

        let pt = ProvisionedThroughput::builder()
            .read_capacity_units(10)
            .write_capacity_units(5)
            .build();

        let attr_type = ScalarAttributeType::from(*column_types.first().unwrap());
        let attribute = AttributeDefinition::builder()
            .attribute_name(primary_key)
            .attribute_type(attr_type)
            .build();

        let resp = client
            .create_table()
            .table_name(table_name)
            .key_schema(ks)
            .provisioned_throughput(pt)
            .attribute_definitions(attribute);

        let _ = resp.send().await?;
        Ok(())
    }
}
