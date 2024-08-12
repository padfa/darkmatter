use clap::Parser;
use clap_verbosity_flag::InfoLevel;
use log::{info, trace};
use rand::distr::Alphanumeric;
use rand::Rng;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio_util::task::TaskTracker;

#[derive(Parser)]
#[command(name = "darkmatter")]
#[command(version = "1.0.0")]
#[command(
    about = "Generates data",
    long_about = "Generates data and churn to test storage solutions."
)]
struct Cli {
    #[command(flatten)]
    verbose: clap_verbosity_flag::Verbosity<InfoLevel>,
    #[arg(short = 'm', long = "numfiles", default_value_t = 128)]
    /// Number of files that should be generated or churned.
    num_files: u16,
    #[arg(long = "min", default_value_t = 8)]
    /// The minimum size used for generating files. IE: All files will meet or be larger than this.
    min_size: u16,
    #[arg(long = "max", default_value_t = 32)]
    /// The maximum size used for generating files. IE: All files will meet or be smaller than this.
    max_size: u16,
    #[arg(default_value = "/opt/darkmatter")]
    directory: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    let cli = Cli::parse();
    let tracker = TaskTracker::new();
    env_logger::Builder::new()
        .filter_level(cli.verbose.log_level_filter())
        .init();
    info!("Darkmatter is starting generation at {}", cli.directory);
    trace!(
        "Generating {} files with random sizes between {}, and {}",
        cli.num_files,
        cli.min_size,
        cli.max_size
    );
    for n in 1..cli.num_files {
        // Write a file.
        tracker.spawn(write_file(n as i32));
    }
    // Once we spawned everything, we close the tracker.
    tracker.close();
    // Wait for everything to finish.
    tracker.wait().await;
    Ok(())
}

async fn write_file(id: i32) -> tokio::io::Result<()> {
    let path: std::string::String = std::string::String::from(id.to_string() + ".txt");
    let mut file: File = tokio::fs::File::create(path).await?;
    {
        while file.metadata().await?.len() < 500000000 {
            let s: std::string::String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(1000000)
                .map(char::from)
                .collect();
            file.write(s.as_bytes()).await?;
        }
    }
    Ok(())
}
