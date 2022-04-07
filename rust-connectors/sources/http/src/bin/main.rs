
use ::http::{HttpConnectorResult, HttpOpt};
use schemars::schema_for;
use structopt::StructOpt;

#[tokio::main]
async fn main() -> HttpConnectorResult<()> {
    if let Some("metadata") = std::env::args().nth(1).as_deref() {
        let schema = schema_for!(HttpOpt);
        let metadata = serde_json::json!({
            "name": env!("CARGO_PKG_NAME"),
            "version": env!("CARGO_PKG_VERSION"),
            "description": env!("CARGO_PKG_DESCRIPTION"),
            "direction": "source",
            "schema": schema,
        });
        let metadata_json = serde_json::to_string_pretty(&metadata).unwrap();
        println!("{}", metadata_json);
        return Ok(());
    }
    let opts: HttpOpt = HttpOpt::from_args();
    opts.execute().await?;

    Ok(())
}
