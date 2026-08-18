use killbot_rust::{location_evidence::run_operator_location_evidence_cli_from_process_args, run};

#[tokio::main]
async fn main() {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if let Some(exit_code) = run_operator_location_evidence_cli_from_process_args(&arguments).await
    {
        std::process::exit(exit_code);
    }
    run().await;
}
