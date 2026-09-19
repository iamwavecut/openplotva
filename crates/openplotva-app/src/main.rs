//! OpenPlotva application entrypoint.

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    match openplotva_app::run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{}", openplotva_app::fatal_error_report(&error));
            ExitCode::FAILURE
        }
    }
}
