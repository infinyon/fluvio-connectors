
use fluvio::metadata::topic::TopicSpec;
use fluvio_connectors_common::opt::CommonSourceOpt;
use fluvio_model_postgres::ReplicationEvent;
use fluvio_model_postgres::{LogicalReplicationMessage, TupleData};
//use postgres_source::{PgConnector, PgConnectorOpt};
//use tokio_postgres::NoTls;
use tokio_stream::StreamExt;
use url::Url;
use postgres_sink::{
    PgConnector,
    PgConnectorOpt,
};

#[tokio::test]
async fn postgres_inserts() -> eyre::Result<()> {
    let fluvio_topic = "postgres-sink".to_string();
    fluvio_future::subscriber::init_logger();
    let postgres_url = std::env::var("FLUVIO_PG_DATABASE_URL")
        .expect("No FLUVIO_PG_DATABASE_URL environment variable found");

    let fluvio = fluvio::Fluvio::connect().await?;
    let admin = fluvio::FluvioAdmin::connect().await?;
    let _ = admin
        .create(
            fluvio_topic.clone(),
            false,
            TopicSpec::new_computed(1, 1, Some(false)),
        )
        .await;
    let producer = fluvio.topic_producer(fluvio_topic.clone()).await?;
    let mut connector = PgConnector::new(
        PgConnectorOpt {
            url: Url::parse(&postgres_url).expect("Failed to parse connector url"),
            resume_timeout: 1000,
            skip_setup: false,
            common: CommonSourceOpt {
                fluvio_topic: fluvio_topic.clone(),
                rust_log: None,
                filter: None,
                map: None,
                arraymap: None,
            },
        }
    ).await?;
    tokio::spawn(async move {
        connector.start().await
    });

    for line in JUMBO_TABLE.lines() {
        producer.send(fluvio::RecordKey::NULL, line).await?;
    }

    Ok(())
}

const _INPUT_RECORDS : &'static str = r#"{"wal_start":24097928,"wal_end":24097928,"timestamp":696613621467512,"message":{"type":"begin","final_lsn":24098592,"timestamp":696613620215019,"xid":735}}
{"wal_start":24098696,"wal_end":24098696,"timestamp":696613621467676,"message":{"type":"commit","flags":0,"commit_lsn":24098592,"end_lsn":24098696,"timestamp":696613620215019}}
{"wal_start":24098744,"wal_end":24098744,"timestamp":696613621732391,"message":{"type":"begin","final_lsn":24266912,"timestamp":696613621710969,"xid":736}}
{"wal_start":24268712,"wal_end":24268712,"timestamp":696613621732916,"message":{"type":"commit","flags":0,"commit_lsn":24266912,"end_lsn":24268712,"timestamp":696613621710969}}
{"wal_start":24268712,"wal_end":24268712,"timestamp":696613621764750,"message":{"type":"begin","final_lsn":24269048,"timestamp":696613621759257,"xid":737}}
{"wal_start":0,"wal_end":0,"timestamp":696613621765195,"message":{"type":"relation","rel_id":16387,"namespace":"public","name":"foo","replica_identity":"Default","columns":[{"flags":1,"name":"id","type_id":23,"type_modifier":-1},{"flags":0,"name":"name","type_id":25,"type_modifier":-1}]}}
{"wal_start":24268816,"wal_end":24268816,"timestamp":696613621765292,"message":{"type":"insert","rel_id":16387,"tuple":[{"Int4":1},{"String":"Fluvio - 1"}]}}
{"wal_start":24269096,"wal_end":24269096,"timestamp":696613621765319,"message":{"type":"commit","flags":0,"commit_lsn":24269048,"end_lsn":24269096,"timestamp":696613621759257}}
{"wal_start":24269096,"wal_end":24269096,"timestamp":696613621864685,"message":{"type":"begin","final_lsn":24269232,"timestamp":696613621847246,"xid":738}}
{"wal_start":24269096,"wal_end":24269096,"timestamp":696613621864706,"message":{"type":"insert","rel_id":16387,"tuple":[{"Int4":2},{"String":"Fluvio - 2"}]}}
{"wal_start":24269280,"wal_end":24269280,"timestamp":696613621864715,"message":{"type":"commit","flags":0,"commit_lsn":24269232,"end_lsn":24269280,"timestamp":696613621847246}}
{"wal_start":24269280,"wal_end":24269280,"timestamp":696613621872997,"message":{"type":"begin","final_lsn":24269416,"timestamp":696613621865297,"xid":739}}
{"wal_start":24269280,"wal_end":24269280,"timestamp":696613621873011,"message":{"type":"insert","rel_id":16387,"tuple":[{"Int4":3},{"String":"Fluvio - 3"}]}}
{"wal_start":24269464,"wal_end":24269464,"timestamp":696613621873017,"message":{"type":"commit","flags":0,"commit_lsn":24269416,"end_lsn":24269464,"timestamp":696613621865297}}
{"wal_start":24269464,"wal_end":24269464,"timestamp":696613621881346,"message":{"type":"begin","final_lsn":24269600,"timestamp":696613621873596,"xid":740}}
{"wal_start":24269464,"wal_end":24269464,"timestamp":696613621881357,"message":{"type":"insert","rel_id":16387,"tuple":[{"Int4":4},{"String":"Fluvio - 4"}]}}
{"wal_start":24269648,"wal_end":24269648,"timestamp":696613621881361,"message":{"type":"commit","flags":0,"commit_lsn":24269600,"end_lsn":24269648,"timestamp":696613621873596}}
{"wal_start":24269648,"wal_end":24269648,"timestamp":696613621889704,"message":{"type":"begin","final_lsn":24269784,"timestamp":696613621881909,"xid":741}}
{"wal_start":24269648,"wal_end":24269648,"timestamp":696613621889715,"message":{"type":"insert","rel_id":16387,"tuple":[{"Int4":5},{"String":"Fluvio - 5"}]}}
{"wal_start":24269832,"wal_end":24269832,"timestamp":696613621889719,"message":{"type":"commit","flags":0,"commit_lsn":24269784,"end_lsn":24269832,"timestamp":696613621881909}}
{"wal_start":24269832,"wal_end":24269832,"timestamp":696613621898017,"message":{"type":"begin","final_lsn":24269968,"timestamp":696613621890250,"xid":742}}
{"wal_start":24269832,"wal_end":24269832,"timestamp":696613621898030,"message":{"type":"insert","rel_id":16387,"tuple":[{"Int4":6},{"String":"Fluvio - 6"}]}}
{"wal_start":24270016,"wal_end":24270016,"timestamp":696613621898035,"message":{"type":"commit","flags":0,"commit_lsn":24269968,"end_lsn":24270016,"timestamp":696613621890250}}
{"wal_start":24270016,"wal_end":24270016,"timestamp":696613621906350,"message":{"type":"begin","final_lsn":24270152,"timestamp":696613621898524,"xid":743}}
{"wal_start":24270016,"wal_end":24270016,"timestamp":696613621906361,"message":{"type":"insert","rel_id":16387,"tuple":[{"Int4":7},{"String":"Fluvio - 7"}]}}
{"wal_start":24270200,"wal_end":24270200,"timestamp":696613621906365,"message":{"type":"commit","flags":0,"commit_lsn":24270152,"end_lsn":24270200,"timestamp":696613621898524}}
{"wal_start":24270200,"wal_end":24270200,"timestamp":696613621914703,"message":{"type":"begin","final_lsn":24270336,"timestamp":696613621906967,"xid":744}}
{"wal_start":24270200,"wal_end":24270200,"timestamp":696613621914714,"message":{"type":"insert","rel_id":16387,"tuple":[{"Int4":8},{"String":"Fluvio - 8"}]}}
{"wal_start":24270384,"wal_end":24270384,"timestamp":696613621914719,"message":{"type":"commit","flags":0,"commit_lsn":24270336,"end_lsn":24270384,"timestamp":696613621906967}}
{"wal_start":24270384,"wal_end":24270384,"timestamp":696613621923021,"message":{"type":"begin","final_lsn":24270520,"timestamp":696613621915257,"xid":745}}
{"wal_start":24270384,"wal_end":24270384,"timestamp":696613621923033,"message":{"type":"insert","rel_id":16387,"tuple":[{"Int4":9},{"String":"Fluvio - 9"}]}}
{"wal_start":24270568,"wal_end":24270568,"timestamp":696613621923037,"message":{"type":"commit","flags":0,"commit_lsn":24270520,"end_lsn":24270568,"timestamp":696613621915257}}"#;
const JUMBO_TABLE : &'static str = r#"
{"wal_start":0,"wal_end":0,"timestamp":696700315397034,"message":{"type":"relation","rel_id":16431,"namespace":"public","name":"better_big_table","replica_identity":"Default","columns":[{"flags":0,"name":"bigint_col","type_id":20,"type_modifier":-1},{"flags":0,"name":"bigserial_col","type_id":20,"type_modifier":-1},{"flags":0,"name":"bit_col","type_id":1560,"type_modifier":1},{"flags":0,"name":"boolean_col","type_id":16,"type_modifier":-1},{"flags":0,"name":"box_col","type_id":603,"type_modifier":-1},{"flags":0,"name":"bytea_col","type_id":17,"type_modifier":-1},{"flags":0,"name":"character_col","type_id":1042,"type_modifier":5},{"flags":0,"name":"cidr_col","type_id":650,"type_modifier":-1},{"flags":0,"name":"circle_col","type_id":718,"type_modifier":-1},{"flags":0,"name":"date_col","type_id":1082,"type_modifier":-1},{"flags":0,"name":"inet_col","type_id":869,"type_modifier":-1},{"flags":0,"name":"integer_col","type_id":23,"type_modifier":-1},{"flags":0,"name":"interval_col","type_id":1186,"type_modifier":-1},{"flags":0,"name":"json_col","type_id":114,"type_modifier":-1},{"flags":0,"name":"jsonb_col","type_id":3802,"type_modifier":-1},{"flags":0,"name":"line_col","type_id":628,"type_modifier":-1},{"flags":0,"name":"lseg_col","type_id":601,"type_modifier":-1},{"flags":0,"name":"macaddr_col","type_id":829,"type_modifier":-1},{"flags":0,"name":"money_col","type_id":790,"type_modifier":-1},{"flags":0,"name":"numeric_col","type_id":1700,"type_modifier":-1},{"flags":0,"name":"path_col","type_id":602,"type_modifier":-1},{"flags":0,"name":"pg_lsn_col","type_id":3220,"type_modifier":-1},{"flags":0,"name":"point_col","type_id":600,"type_modifier":-1},{"flags":0,"name":"polygon_col","type_id":604,"type_modifier":-1},{"flags":0,"name":"real_col","type_id":700,"type_modifier":-1},{"flags":0,"name":"smallint_col","type_id":21,"type_modifier":-1},{"flags":0,"name":"smallserial_col","type_id":21,"type_modifier":-1},{"flags":0,"name":"text_col","type_id":25,"type_modifier":-1},{"flags":0,"name":"time_col","type_id":1083,"type_modifier":-1},{"flags":0,"name":"timestamp_col","type_id":1114,"type_modifier":-1},{"flags":0,"name":"tsquery_col","type_id":3615,"type_modifier":-1},{"flags":0,"name":"tsvector_col","type_id":3614,"type_modifier":-1},{"flags":0,"name":"txid_snapshot_col","type_id":2970,"type_modifier":-1},{"flags":0,"name":"uuid_col","type_id":2950,"type_modifier":-1},{"flags":0,"name":"xml_col","type_id":142,"type_modifier":-1}]}}
{"wal_start":25030928,"wal_end":25030928,"timestamp":696700315397162,"message":{"type":"insert","rel_id":16431,"tuple":[{"Int8":10},{"Int8":1},"Null","Null","Null","Null","Null","Null","Null","Null","Null","Null","Null","Null","Null","Null","Null","Null","Null","Null","Null","Null","Null","Null","Null","Null",{"Int2":1},"Null","Null","Null","Null","Null","Null","Null","Null"]}}
"#;
