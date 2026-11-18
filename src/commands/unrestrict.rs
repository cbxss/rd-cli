use std::fs;
use std::path::Path;

use anyhow::{Context, Result};

use crate::api::Client;
use crate::api::models::UnrestrictedLink;
use crate::cli::UnrestrictArgs;
use crate::download::{self, DownloadOpts, Report};
use crate::output;

pub async fn run(args: UnrestrictArgs) -> Result<()> {
    // Fails fast, before touching the network, if there is no token.
    let client = Client::from_config()?;
    let resolved = resolve(&client, &args).await?;
    let links: Vec<&UnrestrictedLink> = resolved.successes.iter().map(|(_, ul)| ul).collect();

    // -l: write the resulting download URLs to stdout and stop. This overrides
    // -t and -q, per the original tool's documented behavior — no download
    // happens, and no other status output is printed either way.
    if args.list {
        for ul in &links {
            println!("{}", ul.download);
        }
        return resolved.finish_without_download();
    }

    if args.json {
        // Bulk (file) mode always prints an array, even if it only contained a
        // single link; a single link passed directly on the command line
        // prints a bare object.
        if !resolved.bulk {
            if let Some(ul) = links.first() {
                output::print_json(ul)?;
            }
        } else {
            output::print_json(&links)?;
        }
    }

    if args.test {
        // -t: do everything except the actual download.
        if !args.json && !args.quiet {
            for ul in &links {
                println!("{} ({})", ul.filename, output::human_bytes(ul.filesize));
            }
        }
        return resolved.finish_without_download();
    }

    // With --json, stdout carries only the JSON document.
    let quiet = args.quiet || args.json;
    download_resolved(resolved, &args, quiet)
        .await?
        .finish(false)
}

/// The unrestrict step of a run: each link resolved, or its failure (already
/// reported on stderr).
pub struct Resolved {
    /// The argument was a file of links.
    bulk: bool,
    successes: Vec<(String, UnrestrictedLink)>,
    failures: Vec<(String, anyhow::Error)>,
}

impl Resolved {
    pub fn direct_urls(&self) -> impl Iterator<Item = &str> {
        self.successes.iter().map(|(_, ul)| ul.download.as_str())
    }

    pub fn finish_without_download(&self) -> Result<()> {
        if !self.failures.is_empty() {
            let total = self.failures.len() + self.successes.len();
            anyhow::bail!("{} of {total} link(s) failed", self.failures.len());
        }
        Ok(())
    }
}

/// Reads the link argument (a link, or a file of them) and unrestricts each.
pub async fn resolve(client: &Client, args: &UnrestrictArgs) -> Result<Resolved> {
    let bulk = Path::new(&args.link).is_file();
    let links = collect_links(&args.link)?;
    if links.is_empty() {
        anyhow::bail!("no links found in {}", args.link);
    }
    if bulk && args.output_file.is_some() {
        anyhow::bail!("-O/--output-file only works with a single link, not a file of links");
    }

    let mut successes = Vec::new();
    let mut failures = Vec::new();
    for link in links {
        match unrestrict_one(client, &link, args.password.as_deref()).await {
            Ok(ul) => {
                if !args.quiet && !args.list && !args.json {
                    download::status(&format!("unrestricted: {link} -> {}", ul.filename));
                }
                successes.push((link, ul));
            }
            Err(err) => {
                eprintln!("error: failed to unrestrict {link}: {err:#}");
                failures.push((link, err));
            }
        }
    }
    Ok(Resolved {
        bulk,
        successes,
        failures,
    })
}

/// Downloads every resolved link into `-o`, reporting each failure on
/// stderr. Unrestrict failures are included in the report.
pub async fn download_resolved(
    resolved: Resolved,
    args: &UnrestrictArgs,
    quiet: bool,
) -> Result<Report> {
    let mut report = Report::default();
    for (link, err) in &resolved.failures {
        report.add_failure(link, err);
    }
    let out_dir = download::prepare_out_dir(args.output_dir.as_deref())?;
    let opts = DownloadOpts {
        concurrency: args.concurrency,
        quiet,
    };
    // Several links can resolve to the same file name; give each its own.
    let mut claimed = std::collections::HashSet::new();
    for (link, ul) in &resolved.successes {
        // -O is the user's own choice and may include a path; the name
        // Real-Debrid returns is reduced to a bare, portable file name.
        let filename = match &args.output_file {
            Some(name) => name.clone(),
            None => download::unique_name(&mut claimed, &download::safe_filename(&ul.filename)),
        };
        let dest = out_dir.join(filename);
        match download::fetch_file(&ul.download, &ul.filename, ul.chunks, &dest, opts).await {
            Ok(path) => report.add_file(&path),
            Err(err) => {
                eprintln!("error: {err:#}");
                report.add_failure(link, &err);
            }
        }
    }
    Ok(report)
}

async fn unrestrict_one(
    client: &Client,
    link: &str,
    password: Option<&str>,
) -> Result<UnrestrictedLink> {
    let mut form: Vec<(&str, &str)> = vec![("link", link)];
    if let Some(pw) = password {
        form.push(("password", pw));
    }
    let ul = client
        .post_form::<UnrestrictedLink>("/unrestrict/link", &form)
        .await?;
    Ok(ul)
}

/// Resolves the `link` CLI argument into a list of links to process. If it
/// names a real file on disk, each non-blank, non-`#`-comment line is treated
/// as a separate link (bulk mode); otherwise the argument itself is the link.
fn collect_links(link_arg: &str) -> Result<Vec<String>> {
    let path = Path::new(link_arg);
    if path.is_file() {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("failed to read link file {}", path.display()))?;
        Ok(contents
            .lines()
            .map(|line| line.trim_start_matches('\u{feff}').trim())
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(str::to_string)
            .collect())
    } else {
        Ok(vec![link_arg.to_string()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_url_is_not_treated_as_a_file() {
        let links = collect_links("https://example.com/file.zip").unwrap();
        assert_eq!(links, vec!["https://example.com/file.zip".to_string()]);
    }

    #[test]
    fn bulk_file_is_parsed_line_by_line_skipping_blanks_and_comments() {
        let path = std::env::temp_dir().join(format!(
            "rd-unrestrict-test-{}-{:?}.txt",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::write(
            &path,
            "\u{feff}https://a.example.com/1\n\n# a comment\n  https://b.example.com/2  \n",
        )
        .unwrap();

        let result = collect_links(path.to_str().unwrap());
        fs::remove_file(&path).unwrap();

        let links = result.unwrap();
        assert_eq!(
            links,
            vec![
                "https://a.example.com/1".to_string(),
                "https://b.example.com/2".to_string(),
            ]
        );
    }
}
