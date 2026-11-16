use anyhow::{Context, Result};

use crate::api::Client;
use crate::api::models::User;
use crate::config::Config;

pub async fn login() -> Result<()> {
    println!("Create a private API token at https://real-debrid.com/apitoken");
    let token =
        rpassword::prompt_password("Paste your API token: ").context("failed to read token")?;
    let token = token.trim().to_string();
    if token.is_empty() {
        anyhow::bail!("no token provided");
    }

    let client = Client::new(Some(token.clone()))?;
    let user: User = client
        .get_json("/user")
        .await
        .context("token validation failed — check the token and try again")?;

    // A damaged config file is simply replaced.
    let mut config = Config::load_file().unwrap_or_default();
    config.api_token = Some(token);
    config.save()?;

    println!("Logged in as {} ({})", user.username, user.email);
    if std::env::var("RD_TOKEN").is_ok_and(|t| !t.trim().is_empty()) {
        println!("Note: RD_TOKEN is set and takes precedence over the saved token.");
    }
    Ok(())
}

pub fn logout() -> Result<()> {
    if Config::clear_saved_token()? {
        println!("Logged out.");
    } else {
        println!("Not logged in.");
    }
    if std::env::var("RD_TOKEN").is_ok_and(|t| !t.trim().is_empty()) {
        println!("Note: RD_TOKEN is still set, so commands keep using that token.");
    }
    Ok(())
}

pub async fn whoami(json: bool) -> Result<()> {
    let client = Client::from_config()?;
    let user: User = client.get_json("/user").await?;

    if json {
        crate::output::print_json(&user)?;
    } else {
        println!("Username:    {}", user.username);
        println!("Email:       {}", user.email);
        println!("Type:        {}", user.account_type);
        println!(
            "Premium:     {}",
            if user.premium > 0 { "yes" } else { "no" }
        );
        println!("Points:      {}", user.points);
    }
    Ok(())
}
