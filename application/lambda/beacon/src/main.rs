use std::io::ErrorKind;

use lambda_runtime::{service_fn, Error};
use tokio::io;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{fmt, EnvFilter, Registry};

use beacon::beacon_handler;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt_layer = fmt::Layer::default();
    let subscriber = Registry::default().with(env_filter).with(fmt_layer);

    tracing::subscriber::set_global_default(subscriber).map_err(|err| {
        io::Error::new(
            ErrorKind::Other,
            format!("failed to install `tracing` subscriber: {}", err),
        )
    })?;

    let handler = service_fn(beacon_handler);
    lambda_runtime::run(handler).await?;

    Ok(())
}
