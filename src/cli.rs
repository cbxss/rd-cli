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
}

#[derive(Subcommand)]
pub enum TorrentsCommand {
    /// Add a magnet link or .torrent file
    Add {
        magnet_or_file: String,
        #[arg(long)]
        host: Option<String>,
        /// Select files immediately: "all" or a comma-separated list of file ids
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
    /// Select which files to download from a torrent
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
