use anyhow::{bail, Result};
use chrono::{DateTime, NaiveDate, Utc};
use clap::Parser;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_sheets::{get_sheets, service_account_from_env};
use std::collections::BTreeMap;
use std::process::{Command, Stdio};

fn cmd(cmd: &str, args: &[&str], description: &str) -> Result<String> {
    let mut child = Command::new(cmd)
        .args(args)
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;

    let mut stdout = child.stdout.take().unwrap();
    let stdout_handle = std::thread::spawn(move || -> String {
        let mut stdout_buffer = vec![];
        let _ = std::io::copy(&mut stdout, &mut stdout_buffer);
        String::from_utf8_lossy(&stdout_buffer).into()
    });

    let mut stderr = child.stderr.take().unwrap();
    let stderr_handle = std::thread::spawn(move || -> String {
        let mut stderr_buffer = vec![];
        let _ = std::io::copy(&mut stderr, &mut stderr_buffer);
        String::from_utf8_lossy(&stderr_buffer).into()
    });

    let exit_status = child.wait()?;
    if exit_status.success() {
        Ok(stdout_handle.join().unwrap())
    } else {
        let stderr_str = stderr_handle.join().unwrap();
        bail!("{description} failed with: {exit_status}, and stderr = {stderr_str}")
    }
}

//        _ _   _           _
//   __ _(_) |_| |__  _   _| |__
//  / _` | | __| '_ \| | | | '_ \
// | (_| | | |_| | | | |_| | |_) |
//  \__, |_|\__|_| |_|\__,_|_.__/
//  |___/

fn gh_api<RetT: DeserializeOwned>(path: &str) -> Result<RetT> {
    let output_str = cmd("gh", &["api", path], "gh api invocation")?;
    Ok(serde_json::from_str(&output_str)?)
}

#[derive(Debug, Deserialize)]
struct TrafficDataPoint {
    timestamp: DateTime<Utc>,
    count: u64,
    uniques: u64,
}

#[derive(Debug, Deserialize)]
struct Clones {
    clones: Vec<TrafficDataPoint>,
}

#[derive(Debug, Deserialize)]
struct Views {
    views: Vec<TrafficDataPoint>,
}

#[derive(Debug, Deserialize)]
struct Owner {
    login: String,
}

#[derive(Debug, Deserialize)]
struct Repo {
    #[expect(dead_code)]
    name: String,
    #[expect(dead_code)]
    full_name: String,
    owner: Owner,
    #[expect(dead_code)]
    html_url: String,
    #[expect(dead_code)]
    description: String,
}

#[derive(Debug, Deserialize)]
struct Asset {
    name: String,
    download_count: u64,
}

#[derive(Debug, Deserialize)]
struct Release {
    assets: Vec<Asset>,
}

struct GithubStats {
    clones: Clones,
    views: Views,
    forks: Vec<Repo>,
    releases: Vec<Release>,
}

impl GithubStats {
    fn get() -> Result<Self> {
        Ok(Self {
            clones: gh_api("repos/:owner/:repo/traffic/clones")?,
            views: gh_api("repos/:owner/:repo/traffic/views")?,
            forks: gh_api("repos/:owner/:repo/forks")?,
            releases: gh_api("repos/:owner/:repo/releases")?,
        })
    }
}

//      _               _
//  ___| |__   ___  ___| |_ ___
// / __| '_ \ / _ \/ _ \ __/ __|
// \__ \ | | |  __/  __/ |_\__ \
// |___/_| |_|\___|\___|\__|___/

trait HasDay {
    fn day(&self) -> &NaiveDate;
}

#[derive(Debug, Serialize, Deserialize)]
struct TrafficSheetsEntry {
    day: NaiveDate,
    count: u64,
    uniques: u64,
}

impl HasDay for TrafficSheetsEntry {
    fn day(&self) -> &NaiveDate {
        &self.day
    }
}

impl From<TrafficDataPoint> for TrafficSheetsEntry {
    fn from(p: TrafficDataPoint) -> Self {
        Self {
            day: p.timestamp.date_naive(),
            count: p.count,
            uniques: p.uniques,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct DownloadsSheetsEntry {
    day: NaiveDate,
    cargo_maelstrom: u64,
    maelstrom_go_test: u64,
    maelstrom_pytest: u64,
    maelstrom_run: u64,
    maelstrom_client: u64,
    maelstrom_broker: u64,
    maelstrom_worker: u64,
    maelstrom_admin: u64,
}

impl DownloadsSheetsEntry {
    fn new() -> Self {
        Self {
            day: DateTime::<Utc>::from(std::time::SystemTime::now()).date_naive(),
            ..Default::default()
        }
    }
}

impl HasDay for DownloadsSheetsEntry {
    fn day(&self) -> &NaiveDate {
        &self.day
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct ForksSheetsEntry {
    day: NaiveDate,
    forks: u64,
    users: String,
}

impl ForksSheetsEntry {
    fn new() -> Self {
        Self {
            day: DateTime::<Utc>::from(std::time::SystemTime::now()).date_naive(),
            ..Default::default()
        }
    }
}

impl HasDay for ForksSheetsEntry {
    fn day(&self) -> &NaiveDate {
        &self.day
    }
}

const DOCUMENT_ID: &str = "1AW1EMyKAK4wsGEuRBIU7Yh21k0H6SXopvqpOSv5RCvA";

async fn merge_sheet_entries<EntryT: DeserializeOwned + Serialize + HasDay>(
    sheets: &mut google_sheets4::Sheets,
    tab_name: &str,
    data: Vec<EntryT>,
) -> Result<()> {
    let existing: Vec<EntryT> = serde_sheets::read_all(sheets, DOCUMENT_ID, tab_name).await?;

    let combined: BTreeMap<_, _> = existing
        .into_iter()
        .chain(data.into_iter())
        .map(|e| (*e.day(), e))
        .collect();
    let combined: Vec<_> = combined.values().collect();

    serde_sheets::write_page(sheets, DOCUMENT_ID, tab_name, &combined).await?;
    Ok(())
}

fn clone_data(clones: Clones) -> Vec<TrafficSheetsEntry> {
    clones
        .clones
        .into_iter()
        .map(TrafficSheetsEntry::from)
        .collect()
}

fn view_data(views: Views) -> Vec<TrafficSheetsEntry> {
    views
        .views
        .into_iter()
        .map(TrafficSheetsEntry::from)
        .collect()
}

fn downloads_data(releases: Vec<Release>) -> Result<Vec<DownloadsSheetsEntry>> {
    let mut entry = DownloadsSheetsEntry::new();
    for release in releases {
        for asset in release.assets {
            if asset.name.starts_with("cargo-maelstrom") {
                entry.cargo_maelstrom += asset.download_count;
            } else if asset.name.starts_with("maelstrom-go-test") {
                entry.maelstrom_go_test += asset.download_count;
            } else if asset.name.starts_with("maelstrom-pytest") {
                entry.maelstrom_pytest += asset.download_count;
            } else if asset.name.starts_with("maelstrom-run") {
                entry.maelstrom_run += asset.download_count;
            } else if asset.name.starts_with("maelstrom-client") {
                entry.maelstrom_client += asset.download_count;
            } else if asset.name.starts_with("maelstrom-broker") {
                entry.maelstrom_broker += asset.download_count;
            } else if asset.name.starts_with("maelstrom-worker") {
                entry.maelstrom_worker += asset.download_count;
            } else if asset.name.starts_with("maelstrom-admin") {
                entry.maelstrom_admin += asset.download_count;
            } else {
                bail!("unknown asset {}", asset.name);
            }
        }
    }
    Ok(vec![entry])
}

fn fork_count_data(forks: Vec<Repo>) -> Vec<ForksSheetsEntry> {
    let mut entry = ForksSheetsEntry::new();
    entry.forks = forks.len() as u64;
    let fork_users = forks
        .iter()
        .map(|r| r.owner.login.clone())
        .rev()
        .collect::<Vec<_>>();
    entry.users = fork_users.join(", ");
    vec![entry]
}

#[tokio::main]
async fn upload_to_sheets(gh_data: GithubStats) -> Result<()> {
    let service_account = service_account_from_env().unwrap();
    let mut sheets = get_sheets(service_account, Some("target/gsheets_token_cache.json")).await?;

    merge_sheet_entries(&mut sheets, "Clones", clone_data(gh_data.clones)).await?;
    merge_sheet_entries(&mut sheets, "Views", view_data(gh_data.views)).await?;
    merge_sheet_entries(&mut sheets, "Downloads", downloads_data(gh_data.releases)?).await?;
    merge_sheet_entries(&mut sheets, "Forks", fork_count_data(gh_data.forks)).await?;

    Ok(())
}

//                  _
//  _ __ ___   __ _(_)_ __
// | '_ ` _ \ / _` | | '_ \
// | | | | | | (_| | | | | |
// |_| |_| |_|\__,_|_|_| |_|
//

#[derive(Debug, Parser)]
pub struct CliArgs {}

pub fn main(_args: CliArgs) -> Result<()> {
    upload_to_sheets(GithubStats::get()?)?;
    Ok(())
}
