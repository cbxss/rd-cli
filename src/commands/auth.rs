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
    Ok(())
}

pub fn logout() -> Result<()> {
    if Config::clear_saved_token()? {
        println!("Logged out.");
    } else {
        println!("Not logged in.");
    }
    Ok(())
}

pub async fn whoami(json: bool) -> Result<()> {
    let client = Client::from_config()?;
    let user: User = client.get_json("/user").await?;

    if json {
        println!("{}", serde_json::to_string_pretty(&user)?);
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
