use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "rd",
    version,
    about = "real-debrid cli",
    after_help = "Exit codes: 0 success; 1 error; 2 invalid usage; 3 incomplete: a download failed in a way a rerun may fix, or a torrent stalled; rerun the same command to continue (finished files are skipped, partial ones resume)."
)]
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
    /// Unrestrict a hoster link (or a file containing several) and download it; use -l to only print the direct links
    #[command(after_help = RESUME_HELP)]
    Unrestrict(UnrestrictArgs),
    /// Download from a magnet, .torrent file, hoster link, or file of links — one command, auto-detected
    #[command(after_help = RESUME_HELP)]
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
        /// Show each host's current status (up/down) as Real-Debrid reports it
        #[arg(long)]
        status: bool,
        #[arg(long, help = "Print machine-readable JSON")]
        json: bool,
    },
    /// Show traffic / bandwidth usage
    Traffic {
        /// Show per-day traffic for each host instead of the remaining-traffic summary
        #[arg(long)]
        details: bool,
        /// With --details: start date, YYYY-MM-DD (default: a week ago; the period can't exceed 31 days)
        #[arg(long, value_name = "YYYY-MM-DD", requires = "details")]
        from: Option<String>,
        /// With --details: end date, YYYY-MM-DD (default: today)
        #[arg(long, value_name = "YYYY-MM-DD", requires = "details")]
        to: Option<String>,
        #[arg(long, help = "Print machine-readable JSON")]
        json: bool,
    },
    /// Generate shell completions
    Completions { shell: clap_complete::Shell },
}

const RESUME_HELP: &str = "Downloads of 50 MiB or more resume: an interrupted file keeps its full size on disk with a `<name>.rdpart` file beside it recording progress. Rerun the same command to continue; don't delete either file. Smaller files restart from scratch.\n\nWhen progress bars can't be drawn (stderr isn't a terminal: a log file, nohup), a plain progress line is printed every 30s instead of a progress bar.\n\nExit codes: 0 success; 1 error; 2 invalid usage; 3 incomplete, a rerun may finish the job.";

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
    /// Print the unrestricted (direct) links to stdout instead of downloading
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
    /// Name the downloaded file (only when the download is a single file; with a torrent, combine with --select)
    #[arg(short = 'O', long = "output-file", conflicts_with = "list")]
    pub output_file: Option<String>,
    /// Torrents only: download just these files ("all" or comma-separated file ids from `rd torrents info <id>`)
    #[arg(long, value_name = "IDS", value_parser = parse_selection)]
    pub select: Option<String>,
    /// Print the direct download links to stdout instead of downloading (torrents: waits until Real-Debrid has them)
    #[arg(short = 'l', long, conflicts_with = "json")]
    pub list: bool,
    /// Print a JSON summary when done: {"files": [{"path", "bytes"}], "failed": [{"link", "error", "retryable"}]}; implies -q
    #[arg(long)]
    pub json: bool,
    /// Password for protected hoster downloads
    #[arg(short = 'p', long)]
    pub password: Option<String>,
    /// Hoster domain Real-Debrid should use for a magnet/torrent's links (rarely needed)
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

/// `all`, or comma-separated file ids (normalized: no spaces).
fn parse_selection(s: &str) -> Result<String, String> {
    let s = s.trim();
    if s.eq_ignore_ascii_case("all") {
        return Ok("all".to_string());
    }
    let ids: Result<Vec<u64>, _> = s.split(',').map(|x| x.trim().parse::<u64>()).collect();
    match ids {
        Ok(ids) if !ids.is_empty() => {
            Ok(ids.iter().map(u64::to_string).collect::<Vec<_>>().join(","))
        }
        _ => Err(format!(
            "invalid file selection '{s}' (use \"all\" or comma-separated file ids, e.g. 1,4,7)"
        )),
    }
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
    use super::{parse_duration, parse_selection};

    #[test]
    fn selections() {
        assert_eq!(parse_selection("all").as_deref(), Ok("all"));
        assert_eq!(parse_selection(" 3, 7 ,12").as_deref(), Ok("3,7,12"));
        assert!(parse_selection("3,x").is_err());
        assert!(parse_selection("").is_err());
    }
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
        /// A magnet link or a path to a .torrent file
        magnet_or_file: String,
        /// Hoster domain Real-Debrid should use for the torrent's links (rarely needed)
        #[arg(long)]
        host: Option<String>,
        /// Select files once the magnet is converted: "all" or comma-separated file ids (download with `rd dl <id>`)
        #[arg(long)]
        select: Option<String>,
        /// Print the new torrent's id as JSON: {"id": "..."}
        #[arg(long)]
        json: bool,
    },
    /// List torrents
    List {
        /// "active": list only torrents still in progress on Real-Debrid
        #[arg(long, value_name = "active")]
        filter: Option<String>,
        #[arg(long, help = "Print machine-readable JSON")]
        json: bool,
    },
    /// Show torrent details, including file ids for `rd torrents select` / `rd dl --select`
    Info {
        /// Torrent id (from `rd torrents list`)
        id: String,
        #[arg(long, help = "Print machine-readable JSON")]
        json: bool,
    },
    /// Choose which files of a torrent Real-Debrid fetches ("all" or comma-separated file ids); download with `rd dl <id>`
    #[command(
        after_help = "Real-Debrid may deliver a torrent with many files as a single archive (e.g. a .rar). That happens on Real-Debrid's side; rd downloads whatever links it returns."
    )]
    Select {
        /// Torrent id (from `rd torrents list`)
        id: String,
        /// "all", or comma-separated file ids (from `rd torrents info <id>`)
        files: String,
    },
    /// Delete a torrent
    Delete {
        /// Torrent id (from `rd torrents list`)
        id: String,
    },
}

#[derive(Subcommand)]
pub enum DownloadsCommand {
    /// List download history
    List {
        #[arg(long, help = "Print machine-readable JSON")]
        json: bool,
    },
    /// Delete a download history entry
    Delete {
        /// Download id (from `rd downloads list`)
        id: String,
    },
}
