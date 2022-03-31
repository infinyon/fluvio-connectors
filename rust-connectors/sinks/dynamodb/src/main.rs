use aws_sdk_dynamodb::{
    model::{
        AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ProvisionedThroughput,
        ScalarAttributeType,
    },
    Client, Endpoint,
};
use fluvio_connectors_common::opt::{CommonSourceOpt, Record};
use fluvio_future::tracing::info;
use http::Uri;
use schemars::{schema_for, JsonSchema};
use structopt::StructOpt;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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
    let _ = opts.execute().await?;
    Ok(())
}

#[derive(StructOpt, Debug, JsonSchema, Clone)]
pub struct DynamoDbOpt {
    #[structopt(long)]
    pub local_dynamodb: bool,

    #[structopt(long)]
    pub table_name: String,

    #[structopt(long)]
    pub column_names: Vec<String>,

    #[structopt(long)]
    pub column_types: Vec<String>,

    #[structopt(long)]
    pub schema_key: Option<String>,

    #[structopt(flatten)]
    #[schemars(flatten)]
    pub common: CommonSourceOpt,
}

impl DynamoDbOpt {
    pub async fn execute(&self) -> anyhow::Result<()> {
        let config = aws_config::load_from_env().await;
        let mut builder = aws_sdk_dynamodb::config::Builder::from(&config);
        if self.local_dynamodb {
            builder = builder.endpoint_resolver(
                // 8000 is the default dynamodb port
                Endpoint::mutable(Uri::from_static("http://localhost:8000")),
            )
        }

        let dynamodb_local_config = builder.build();

        let client = Client::from_conf(dynamodb_local_config);
        let _ = self.create_table(&client).await?;

        let mut stream = self.common.create_consumer_stream(0).await?;
        info!("Starting stream");
        while let Some(Ok(record)) = stream.next().await {
            let _ = self.send_to_dynamodb(&record, &client).await;
        }
        Ok(())
    }
    pub async fn send_to_dynamodb(
        &self,
        record: &Record,
        client: &aws_sdk_dynamodb::Client,
    ) -> anyhow::Result<()> {
        use serde_json::value::Value;
        let json: Value = serde_json::from_slice(record.value())?;
        let mut request = client.put_item().table_name(&self.table_name);

        for column in &self.column_names {
            if let Some(value) = json.get(column) {
                let attr = if value.is_string() {
                    AttributeValue::S(value.to_string())
                } else {
                    unimplemented!("This case for a string isn't implemented yet!");
                };
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
        let column_names = &self.column_names;
        let column_types = &self.column_types;
        if column_names.is_empty() {
            panic!("Must have one ore more columns");
        }
        if column_names.len() != column_types.len() {
            panic!("Must have the same number of column names as column types");
        }
        let first_column = column_names.first().unwrap().to_string();

        let primary_key = self.schema_key.as_ref().unwrap_or(&first_column);

        if !column_names.contains(primary_key) {
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
        let mut resp = client
            .create_table()
            .table_name(table_name)
            .key_schema(ks)
            .provisioned_throughput(pt);

        for (attribute_name, attribute_type) in column_names.iter().zip(column_types.iter()) {
            let attr_type = ScalarAttributeType::from(attribute_type.as_str());
            let attribute = AttributeDefinition::builder()
                .attribute_name(attribute_name.to_string())
                .attribute_type(attr_type)
                .build();
            resp = resp.attribute_definitions(attribute);
        }

        let _ = resp.send().await?;
        Ok(())
    }
}
