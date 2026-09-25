//! A minimal Google Sheets client: just enough to read and replace the contents of a tab, as rows
//! of serializable objects. It authenticates as a Google Cloud service account.
//!
//! Rows are converted to and from objects with the `csv` crate, with the first row of a tab being
//! the header row.

use anyhow::{Context as _, Result};
use chrono::Utc;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::json;
use url::Url;

const SHEETS_API: &str = "https://sheets.googleapis.com/v4/spreadsheets/";
const SCOPE: &str = "https://www.googleapis.com/auth/spreadsheets";
const TOKEN_LIFETIME_SECS: i64 = 60 * 60;

/// The parts of a service account key file that we need.
#[derive(Deserialize)]
struct ServiceAccountKey {
    client_email: String,
    private_key: String,
    token_uri: String,
}

/// The claims of the JWT we exchange for an access token. See
/// <https://developers.google.com/identity/protocols/oauth2/service-account#httprest>.
#[derive(Debug, PartialEq, Serialize)]
struct Claims {
    iss: String,
    scope: String,
    aud: String,
    iat: i64,
    exp: i64,
}

impl Claims {
    fn new(key: &ServiceAccountKey, now: i64) -> Self {
        Self {
            iss: key.client_email.clone(),
            scope: SCOPE.into(),
            aud: key.token_uri.clone(),
            iat: now,
            exp: now + TOKEN_LIFETIME_SECS,
        }
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

#[derive(Deserialize)]
struct ValueRange {
    #[serde(default)]
    values: Vec<Vec<String>>,
}

pub struct Sheets {
    client: reqwest::Client,
    access_token: String,
}

impl Sheets {
    /// Authenticate as the service account whose key file contents are in the
    /// `SERVICE_ACCOUNT_JSON` environment variable.
    pub async fn from_env() -> Result<Self> {
        let key: ServiceAccountKey = serde_json::from_str(
            &std::env::var("SERVICE_ACCOUNT_JSON").context("reading SERVICE_ACCOUNT_JSON")?,
        )
        .context("parsing SERVICE_ACCOUNT_JSON")?;
        let jwt = jsonwebtoken::encode(
            &Header::new(Algorithm::RS256),
            &Claims::new(&key, Utc::now().timestamp()),
            &EncodingKey::from_rsa_pem(key.private_key.as_bytes())?,
        )?;

        let client = reqwest::Client::new();
        let response: TokenResponse = client
            .post(&key.token_uri)
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", &jwt),
            ])
            .send()
            .await?
            .error_for_status()
            .context("getting a Google access token")?
            .json()
            .await?;
        Ok(Self {
            client,
            access_token: response.access_token,
        })
    }

    fn values_url(document_id: &str, range: &str) -> Url {
        let mut url = Url::parse(SHEETS_API).unwrap();
        url.path_segments_mut()
            .unwrap()
            .pop_if_empty()
            .extend([document_id, "values", range]);
        url
    }

    /// Read all of the rows of the tab `tab_name` in document `document_id`, using the first row
    /// as headers. Rows that can't be deserialized are reported and skipped.
    pub async fn read_all<T: DeserializeOwned>(
        &self,
        document_id: &str,
        tab_name: &str,
    ) -> Result<Vec<T>> {
        let value_range: ValueRange = self
            .client
            .get(Self::values_url(document_id, tab_name))
            .bearer_auth(&self.access_token)
            .send()
            .await?
            .error_for_status()
            .with_context(|| format!("reading tab {tab_name}"))?
            .json()
            .await?;
        rows_to_objects(value_range.values)
    }

    /// Replace the contents of the tab `tab_name` in document `document_id` with the given
    /// objects, preceded by a header row.
    pub async fn write_page(
        &self,
        document_id: &str,
        tab_name: &str,
        objects: &[impl Serialize],
    ) -> Result<()> {
        let rows = objects_to_rows(objects)?;

        self.client
            .post(Self::values_url(document_id, &format!("{tab_name}:clear")))
            .bearer_auth(&self.access_token)
            .json(&json!({}))
            .send()
            .await?
            .error_for_status()
            .with_context(|| format!("clearing tab {tab_name}"))?;

        let mut url = Self::values_url(document_id, tab_name);
        url.query_pairs_mut()
            .append_pair("valueInputOption", "USER_ENTERED")
            .append_pair("includeValuesInResponse", "false");
        self.client
            .put(url)
            .bearer_auth(&self.access_token)
            .json(&json!({ "range": tab_name, "values": rows }))
            .send()
            .await?
            .error_for_status()
            .with_context(|| format!("writing tab {tab_name}"))?;
        Ok(())
    }
}

/// Deserialize rows, the first of which is the header row, into objects.
fn rows_to_objects<T: DeserializeOwned>(rows: Vec<Vec<String>>) -> Result<Vec<T>> {
    // The Sheets API leaves trailing empty cells off of rows, so put them back.
    let width = rows.first().map(Vec::len).unwrap_or_default();
    let mut writer = csv::Writer::from_writer(vec![]);
    for mut row in rows {
        if row.len() < width {
            row.resize(width, String::new());
        }
        writer.write_record(&row)?;
    }
    let data = writer.into_inner()?;

    let mut objects = vec![];
    for result in csv::Reader::from_reader(&data[..]).deserialize() {
        match result {
            Ok(object) => objects.push(object),
            Err(err) => println!("error deserializing row: {err}"),
        }
    }
    Ok(objects)
}

/// Serialize objects into rows, the first of which is the header row.
fn objects_to_rows(objects: &[impl Serialize]) -> Result<Vec<Vec<String>>> {
    let mut writer = csv::Writer::from_writer(vec![]);
    for object in objects {
        writer.serialize(object)?;
    }
    let data = writer.into_inner()?;

    csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(&data[..])
        .records()
        .map(|record| Ok(record?.iter().map(ToOwned::to_owned).collect()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Entry {
        day: NaiveDate,
        count: u64,
        users: String,
    }

    fn entries() -> Vec<Entry> {
        vec![
            Entry {
                day: NaiveDate::from_ymd_opt(2026, 9, 24).unwrap(),
                count: 3,
                users: "alice, bob".into(),
            },
            Entry {
                day: NaiveDate::from_ymd_opt(2026, 9, 25).unwrap(),
                count: 0,
                users: String::new(),
            },
        ]
    }

    fn row(cells: &[&str]) -> Vec<String> {
        cells.iter().map(|&s| s.to_owned()).collect()
    }

    #[test]
    fn objects_to_rows_has_header_row() {
        assert_eq!(
            objects_to_rows(&entries()).unwrap(),
            vec![
                row(&["day", "count", "users"]),
                row(&["2026-09-24", "3", "alice, bob"]),
                row(&["2026-09-25", "0", ""]),
            ]
        );
    }

    #[test]
    fn rows_round_trip() {
        let rows = objects_to_rows(&entries()).unwrap();
        assert_eq!(rows_to_objects::<Entry>(rows).unwrap(), entries());
    }

    #[test]
    fn empty_tab_has_no_objects() {
        assert_eq!(rows_to_objects::<Entry>(vec![]).unwrap(), vec![]);
    }

    #[test]
    fn missing_trailing_cells_are_empty() {
        let rows = vec![
            row(&["day", "count", "users"]),
            row(&["2026-09-24", "3", "alice, bob"]),
            row(&["2026-09-25", "0"]),
        ];
        assert_eq!(rows_to_objects::<Entry>(rows).unwrap(), entries());
    }

    #[test]
    fn bad_rows_are_skipped() {
        let rows = vec![
            row(&["day", "count", "users"]),
            row(&["2026-09-24", "not a number", "alice"]),
            row(&["2026-09-26", "1", "bob"]),
        ];
        assert_eq!(
            rows_to_objects::<Entry>(rows).unwrap(),
            vec![Entry {
                day: NaiveDate::from_ymd_opt(2026, 9, 26).unwrap(),
                count: 1,
                users: "bob".into(),
            }]
        );
    }

    #[test]
    fn claims() {
        let key = ServiceAccountKey {
            client_email: "stats@example.iam.gserviceaccount.com".into(),
            private_key: String::new(),
            token_uri: "https://oauth2.googleapis.com/token".into(),
        };
        assert_eq!(
            Claims::new(&key, 1_000),
            Claims {
                iss: "stats@example.iam.gserviceaccount.com".into(),
                scope: "https://www.googleapis.com/auth/spreadsheets".into(),
                aud: "https://oauth2.googleapis.com/token".into(),
                iat: 1_000,
                exp: 4_600,
            }
        );
    }

    #[test]
    fn values_url() {
        assert_eq!(
            Sheets::values_url("doc-id", "Clones").as_str(),
            "https://sheets.googleapis.com/v4/spreadsheets/doc-id/values/Clones"
        );
        assert_eq!(
            Sheets::values_url("doc-id", "Clones:clear").as_str(),
            "https://sheets.googleapis.com/v4/spreadsheets/doc-id/values/Clones:clear"
        );
    }
}
