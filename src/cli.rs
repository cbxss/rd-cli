use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(name = "rd", version, about = "real-debrid cli")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Manage authentication
    Auth {
        #[command(subcommand)]
        command: AuthCommand,
    },
    /// Unrestrict a hoster link (or a file containing several) and download it
    Unrestrict(UnrestrictArgs),
    /// Download from a magnet, .torrent file, hoster link, or file of links — one command, auto-detected
    Dl(DlArgs),
    /// Manage torrents
    Torrents {
        #[command(subcommand)]
        command: TorrentsCommand,
    },
    /// Manage download history
    Downloads {
        #[command(subcommand)]
        command: DownloadsCommand,
    },
    /// List supported hosts
    Hosts {
        #[arg(long)]
        status: bool,
        #[arg(long, help = "Print machine-readable JSON")]
        json: bool,
    },
    /// Show traffic / bandwidth usage
    Traffic {
        #[arg(long)]
        details: bool,
        #[arg(long)]
        from: Option<String>,
        #[arg(long)]
        to: Option<String>,
        #[arg(long, help = "Print machine-readable JSON")]
        json: bool,
    },
    /// Generate shell completions
    Completions { shell: clap_complete::Shell },
}

#[derive(Subcommand)]
pub enum AuthCommand {
    /// Save your Real-Debrid private API token (from real-debrid.com/apitoken)
    Login,
    /// Remove the saved API token
    Logout,
    /// Show the currently authenticated account
    Whoami {
        #[arg(long, help = "Print machine-readable JSON")]
        json: bool,
    },
}

#[derive(Args)]
pub struct UnrestrictArgs {
    /// A hoster link, or a path to a file containing one link per line
    pub link: String,
    /// Download into this directory instead of the current one
    #[arg(short = 'o', long = "output-dir")]
    pub output_dir: Option<String>,
    /// Name the downloaded file (only valid for a single link)
    #[arg(short = 'O', long = "output-file")]
    pub output_file: Option<String>,
    /// Password for protected downloads
    #[arg(short = 'p', long)]
    pub password: Option<String>,
    /// Perform all operations except the actual download
    #[arg(short = 't', long)]
    pub test: bool,
    /// Suppress output
    #[arg(short = 'q', long)]
    pub quiet: bool,
    /// Print the unrestricted links to stdout instead of downloading
    #[arg(short = 'l', long)]
    pub list: bool,
    #[arg(long, help = "Print machine-readable JSON")]
    pub json: bool,
    /// Number of concurrent connections for large, range-capable downloads
    #[arg(long, default_value_t = 8)]
    pub concurrency: usize,
}

#[derive(Args)]
pub struct DlArgs {
    /// A magnet link, a path to a .torrent file, a hoster link, or a path to a file listing several hoster links
    pub link: String,
    /// Download into this directory instead of the current one
    #[arg(short = 'o', long = "output-dir")]
    pub output_dir: Option<String>,
    /// Password for protected hoster downloads
    #[arg(short = 'p', long)]
    pub password: Option<String>,
    /// Preferred host for magnet/torrent uploads
    #[arg(long)]
    pub host: Option<String>,
    /// Suppress status output and progress bars
    #[arg(short = 'q', long)]
    pub quiet: bool,
    /// Number of concurrent connections for large, range-capable downloads
    #[arg(long, default_value_t = 8)]
    pub concurrency: usize,
    /// Number of files to download at the same time (torrents with several files)
    #[arg(short = 'j', long, default_value_t = 3)]
    pub jobs: usize,
    /// Give up on a torrent after this long without progress: e.g. 30m, 2h, 90s; a bare number is minutes; 0 waits forever
    #[arg(long, value_name = "DURATION", default_value = "30m", value_parser = parse_duration)]
    pub stall_timeout: std::time::Duration,
}

/// Parses `90s`, `30m`, `2h`, or a bare number of minutes.
fn parse_duration(s: &str) -> Result<std::time::Duration, String> {
    let s = s.trim();
    let (num, mult) = match s.char_indices().last() {
        Some((i, 's')) => (&s[..i], 1),
        Some((i, 'm')) => (&s[..i], 60),
        Some((i, 'h')) => (&s[..i], 3600),
        _ => (s, 60),
    };
    num.trim()
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(mult))
        .map(std::time::Duration::from_secs)
        .ok_or_else(|| format!("invalid duration '{s}' (use e.g. 90s, 30m, 2h)"))
}

#[cfg(test)]
mod tests {
    use super::parse_duration;
    use std::time::Duration;

    #[test]
    fn durations() {
        assert_eq!(parse_duration("30"), Ok(Duration::from_secs(1800)));
        assert_eq!(parse_duration("90s"), Ok(Duration::from_secs(90)));
        assert_eq!(parse_duration("2h"), Ok(Duration::from_secs(7200)));
        assert_eq!(parse_duration("0"), Ok(Duration::ZERO));
        assert!(parse_duration("soon").is_err());
        assert!(parse_duration("999999999999999999h").is_err());
    }
}

#[derive(Subcommand)]
pub enum TorrentsCommand {
    /// Add a magnet link or .torrent file
    Add {
        magnet_or_file: String,
        #[arg(long)]
        host: Option<String>,
        /// Select files once the magnet is converted: "all" or comma-separated file ids (download with `rd dl <id>`)
        #[arg(long)]
        select: Option<String>,
    },
    /// List torrents
    List {
        #[arg(long)]
        filter: Option<String>,
        #[arg(long, help = "Print machine-readable JSON")]
        json: bool,
    },
    /// Show torrent details
    Info { id: String },
    /// Choose which files of a torrent Real-Debrid fetches ("all" or comma-separated file ids); download with `rd dl <id>`
    Select { id: String, files: String },
    /// Delete a torrent
    Delete { id: String },
}

#[derive(Subcommand)]
pub enum DownloadsCommand {
    /// List download history
    List {
        #[arg(long, help = "Print machine-readable JSON")]
        json: bool,
    },
    /// Delete a download history entry
    Delete { id: String },
}
