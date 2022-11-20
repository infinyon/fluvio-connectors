use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tide::prelude::*;
use tide::Request;

#[derive(Clone)]
struct State {
    get_count: Arc<AtomicU32>,
    post_count: Arc<AtomicU32>,
}
impl State {
    fn new() -> Self {
        Self {
            get_count: Arc::new(AtomicU32::new(0)),
            post_count: Arc::new(AtomicU32::new(0)),
        }
    }
}

#[async_std::main]
async fn main() -> tide::Result<()> {
    let mut app = tide::with_state(State::new());
    app.at("/get").get(get_request);
    app.at("/time").get(get_time_request);
    app.at("/post").post(post_request);
    app.listen("0.0.0.0:8080").await?;
    Ok(())
}
async fn get_request(req: Request<State>) -> tide::Result {
    let state = req.state();
    let value = state.get_count.fetch_add(1, Ordering::Relaxed) + 1;
    Ok(format!("Hello, Fluvio! - {}", value).into())
}
use std::time::{SystemTime, UNIX_EPOCH};

async fn get_time_request(_req: Request<State>) -> tide::Result {
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    Ok(format!("{}", time).into())
}

#[derive(Debug, Deserialize)]
struct HelloPostBody {
    name: String,
}

async fn post_request(mut req: Request<State>) -> tide::Result {
    let HelloPostBody { name } = req.body_json().await?;
    let state = req.state();
    let value = state.post_count.fetch_add(1, Ordering::Relaxed) + 1;
    Ok(format!("Hello, {}! - {}", name, value).into())
}
