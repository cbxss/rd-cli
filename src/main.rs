mod api;
mod cli;
mod commands;
mod config;
mod download;
mod http;
mod output;
mod torrent;

use anyhow::Result;
use clap::{CommandFactory, Parser};
use clap_complete::generate;

use cli::{AuthCommand, Cli, Commands, DownloadsCommand, TorrentsCommand};

#[tokio::main]
async fn main() {
    // Exit quietly when stdout is closed early (`rd torrents list | head`),
    // like other Unix tools, instead of panicking on the failed write.
    #[cfg(unix)]
    // SAFETY: restoring the default disposition of a signal at startup.
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }
    if let Err(err) = run().await {
        eprintln!("error: {err:#}");
        // 3: rerunning the same command can finish the job; 1: anything else.
        // (clap exits 2 for usage errors.)
        let rerun = err
            .chain()
            .any(|e| e.downcast_ref::<download::Incomplete>().is_some());
        std::process::exit(if rerun {
            download::Incomplete::EXIT_CODE
        } else {
            1
        });
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Auth { command } => match command {
            AuthCommand::Login => commands::auth::login().await,
            AuthCommand::Logout => commands::auth::logout(),
            AuthCommand::Whoami { json } => commands::auth::whoami(json).await,
        },
        Commands::Completions { shell } => {
            let mut cmd = Cli::command();
            let name = cmd.get_name().to_string();
            generate(shell, &mut cmd, name, &mut std::io::stdout());
            Ok(())
        }
        Commands::Unrestrict(args) => commands::unrestrict::run(args).await,
        Commands::Dl(args) => commands::dl::run(args).await,
        Commands::Torrents { command } => match command {
            TorrentsCommand::Add {
                magnet_or_file,
                host,
                select,
                json,
            } => commands::torrents::add(magnet_or_file, host, select, json).await,
            TorrentsCommand::List { filter, json } => commands::torrents::list(filter, json).await,
            TorrentsCommand::Info { id, json } => commands::torrents::info(id, json).await,
            TorrentsCommand::Select { id, files } => commands::torrents::select(id, files).await,
            TorrentsCommand::Delete { id } => commands::torrents::delete(id).await,
        },
        Commands::Downloads { command } => match command {
            DownloadsCommand::List { json } => commands::downloads::list(json).await,
            DownloadsCommand::Delete { id } => commands::downloads::delete(id).await,
        },
        Commands::Hosts { status, json } => commands::hosts::list(status, json).await,
        Commands::Traffic {
            details,
            from,
            to,
            json,
        } => commands::traffic::show(details, from, to, json).await,
    }
}
