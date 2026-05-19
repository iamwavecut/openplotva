//! OpenPlotva application entrypoint.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    openplotva_app::run().await
}
