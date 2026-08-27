//! Generates `config/stargates.json`: the undirected k-space stargate
//! adjacency map used by the sov timer feed's reachability engine
//! (`src/sov_feed/graph.rs`; spec "Reachability",
//! `.scratch/esi-intel-feeds/spec.md`; ticket
//! `.scratch/esi-intel-feeds/issues/04-stargate-reachability-and-sov-timers-board.md`).
//!
//! ## What it does
//!
//! 1. Fetches the Fuzzwork SDE CSV dump's directory listing
//!    (`https://www.fuzzwork.co.uk/dump/latest/`) to discover a version
//!    identifier for the snapshot (a `eve_<build>_<date>_<time>` stamped
//!    filename, e.g. `eve_3480926_20260826_134500`). If that identifier
//!    cannot be found on the page, generation still proceeds -- the
//!    version falls back to `unknown (see source_last_modified)` and the
//!    CSV response's `Last-Modified` header is recorded instead, so the
//!    file is never left without *some* provenance.
//! 2. Fetches `https://www.fuzzwork.co.uk/dump/latest/csv/mapSolarSystemJumps.csv`,
//!    a plain (uncompressed) CSV Fuzzwork publishes alongside the
//!    `.bz2` -- using it avoids adding a bzip2 dependency to this crate
//!    for a one-off, manually-triggered generator. Each row is one
//!    directed stargate connection (`fromSolarSystemID`,
//!    `toSolarSystemID`); the live table already lists both directions of
//!    every connection, but this generator adds both directions
//!    unconditionally regardless of what the source contains, so the
//!    output is guaranteed symmetric even if that ever changes upstream.
//! 3. Writes `config/stargates.json`: `sde_version`,
//!    `source_last_modified`, `generated_at` (this run's time),
//!    `system_count`, `edge_count` (undirected), and `adjacency` (system
//!    ID -> sorted neighbour IDs).
//!
//! ## Refreshing on SDE releases
//!
//! Re-run `cargo run --bin build_stargate_graph` and commit the result
//! whenever CCP ships an SDE update that could move a stargate (rare, but
//! it has happened around Pochven and other structural changes). The tool
//! is idempotent and network-only (no local state); it always downloads
//! the current `latest` dump. Review the diff before committing --
//! `system_count`/`edge_count` should move by at most a handful of
//! systems for an ordinary release.
//!
//! No CSV-parsing crate is used: Fuzzwork's `mapSolarSystemJumps.csv` rows
//! are plain double-quoted, comma-separated integers with no embedded
//! commas, quotes, or newlines, so a manual `split(',')` parse below is
//! sufficient and avoids adding a dependency for one generator binary.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

const DUMP_INDEX_URL: &str = "https://www.fuzzwork.co.uk/dump/latest/";
const JUMPS_CSV_URL: &str = "https://www.fuzzwork.co.uk/dump/latest/csv/mapSolarSystemJumps.csv";
const OUTPUT_PATH: &str = "config/stargates.json";
const FETCH_TIMEOUT: Duration = Duration::from_secs(60);
const USER_AGENT: &str = "killbot-rust stargate graph generator";

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("build_stargate_graph failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let client = reqwest::Client::builder()
        .timeout(FETCH_TIMEOUT)
        .user_agent(USER_AGENT)
        .build()
        .map_err(|error| format!("could not build HTTP client: {error}"))?;

    let sde_version = discover_sde_version(&client)
        .await
        .unwrap_or_else(|error| {
            eprintln!("warning: could not discover an SDE version stamp ({error}); falling back to source_last_modified only");
            "unknown".to_string()
        });

    let response = client
        .get(JUMPS_CSV_URL)
        .send()
        .await
        .map_err(|error| format!("could not fetch {JUMPS_CSV_URL}: {error}"))?;
    if !response.status().is_success() {
        return Err(format!(
            "{JUMPS_CSV_URL} returned HTTP {}",
            response.status()
        ));
    }
    let source_last_modified = response
        .headers()
        .get(reqwest::header::LAST_MODIFIED)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let body = response
        .text()
        .await
        .map_err(|error| format!("could not read {JUMPS_CSV_URL} body: {error}"))?;

    let edges = parse_jump_edges(&body)?;
    if edges.is_empty() {
        return Err(
            "parsed zero stargate edges; refusing to overwrite the existing graph".to_string(),
        );
    }
    let adjacency = build_symmetric_adjacency(&edges);
    let system_count = adjacency.len();
    let edge_count: usize = adjacency.values().map(Vec::len).sum::<usize>() / 2;

    let file = killbot_rust::sov_feed::graph::StargateGraphFile {
        sde_version,
        source_last_modified,
        generated_at: chrono::Utc::now(),
        system_count,
        edge_count,
        adjacency,
    };

    let json = serde_json::to_string_pretty(&file)
        .map_err(|error| format!("could not serialize the stargate graph: {error}"))?;
    std::fs::write(Path::new(OUTPUT_PATH), json)
        .map_err(|error| format!("could not write {OUTPUT_PATH}: {error}"))?;

    println!(
        "wrote {OUTPUT_PATH}: sde_version={}, source_last_modified={}, systems={system_count}, edges={edge_count}",
        file.sde_version,
        file.source_last_modified.as_deref().unwrap_or("(none)")
    );
    Ok(())
}

/// Scrapes the Fuzzwork dump directory listing for a `eve_<digits>_<digits>_<digits>`
/// stamped filename (e.g. `eve_3480926_20260826_134500.db.gz`), which is
/// the closest thing that index offers to an SDE release identifier (there
/// is no `sde-YYYYMMDD-TRANQUILITY`-style name published there). Plain
/// string scanning, not a regex crate: the pattern is fixed and simple
/// enough that adding a dependency for it is not worth it.
async fn discover_sde_version(client: &reqwest::Client) -> Result<String, String> {
    let body = client
        .get(DUMP_INDEX_URL)
        .send()
        .await
        .map_err(|error| format!("could not fetch {DUMP_INDEX_URL}: {error}"))?
        .text()
        .await
        .map_err(|error| format!("could not read {DUMP_INDEX_URL} body: {error}"))?;
    extract_sde_version_stamp(&body).ok_or_else(|| {
        "no eve_<build>_<date>_<time> filename found on the dump index page".to_string()
    })
}

fn extract_sde_version_stamp(html: &str) -> Option<String> {
    let start = html.find("eve_")?;
    let rest = &html[start..];
    let end = rest.find(['.', '"'])?;
    let candidate = &rest[..end];
    // Sanity-check the shape (eve_<digits>_<digits>_<digits>) rather than
    // trusting the first "eve_" occurrence blindly.
    let mut parts = candidate.splitn(4, '_');
    let literal = parts.next()?;
    let build = parts.next()?;
    let date = parts.next()?;
    let time = parts.next()?;
    if literal == "eve"
        && !build.is_empty()
        && build.chars().all(|c| c.is_ascii_digit())
        && date.len() == 8
        && date.chars().all(|c| c.is_ascii_digit())
        && time.len() == 6
        && time.chars().all(|c| c.is_ascii_digit())
    {
        Some(candidate.to_string())
    } else {
        None
    }
}

/// Parses `mapSolarSystemJumps.csv`'s `fromSolarSystemID`/`toSolarSystemID`
/// columns into directed edges. Tolerates (skips, with a warning) any row
/// that does not have the expected column count or integer values, rather
/// than failing the whole run over one malformed row.
fn parse_jump_edges(csv: &str) -> Result<Vec<(i64, i64)>, String> {
    let mut lines = csv.lines();
    let header = lines
        .next()
        .ok_or_else(|| "empty CSV response".to_string())?
        .trim_start_matches('\u{feff}');
    let columns: Vec<&str> = header.split(',').map(unquote).collect();
    let from_index = columns
        .iter()
        .position(|c| *c == "fromSolarSystemID")
        .ok_or_else(|| "CSV header is missing fromSolarSystemID".to_string())?;
    let to_index = columns
        .iter()
        .position(|c| *c == "toSolarSystemID")
        .ok_or_else(|| "CSV header is missing toSolarSystemID".to_string())?;

    let mut edges = Vec::new();
    for (line_number, line) in lines.enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split(',').map(unquote).collect();
        let (Some(from_raw), Some(to_raw)) = (fields.get(from_index), fields.get(to_index)) else {
            eprintln!(
                "warning: skipping malformed CSV row {}: {line}",
                line_number + 2
            );
            continue;
        };
        match (from_raw.parse::<i64>(), to_raw.parse::<i64>()) {
            (Ok(from), Ok(to)) => edges.push((from, to)),
            _ => {
                eprintln!(
                    "warning: skipping non-integer CSV row {}: {line}",
                    line_number + 2
                );
            }
        }
    }
    Ok(edges)
}

fn unquote(field: &str) -> &str {
    field.trim().trim_matches('"')
}

/// Builds a deduplicated, symmetric adjacency map: every `(a, b)` input
/// edge produces both `a -> b` and `b -> a`, and each neighbour list is
/// sorted for deterministic output.
fn build_symmetric_adjacency(edges: &[(i64, i64)]) -> BTreeMap<i64, Vec<i64>> {
    let mut adjacency: BTreeMap<i64, std::collections::BTreeSet<i64>> = BTreeMap::new();
    for &(a, b) in edges {
        adjacency.entry(a).or_default().insert(b);
        adjacency.entry(b).or_default().insert(a);
    }
    adjacency
        .into_iter()
        .map(|(system, neighbors)| (system, neighbors.into_iter().collect()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_jump_edges_reads_the_fuzzwork_column_shape() {
        let csv = "\u{feff}\"fromRegionID\",\"fromConstellationID\",\"fromSolarSystemID\",\"toSolarSystemID\",\"toConstellationID\",\"toRegionID\"\n\"10000001\",\"20000001\",\"30000001\",\"30000003\",\"20000001\",\"10000001\"\n";
        let edges = parse_jump_edges(csv).expect("parse fixture CSV");
        assert_eq!(edges, vec![(30_000_001, 30_000_003)]);
    }

    #[test]
    fn parse_jump_edges_skips_malformed_rows_without_failing() {
        let csv = "\"fromSolarSystemID\",\"toSolarSystemID\"\n\"30000001\",\"30000003\"\n\"not-an-id\",\"30000005\"\n\"30000007\"\n";
        let edges = parse_jump_edges(csv).expect("parse fixture CSV with bad rows");
        assert_eq!(edges, vec![(30_000_001, 30_000_003)]);
    }

    #[test]
    fn build_symmetric_adjacency_is_undirected_deduplicated_and_sorted() {
        let adjacency = build_symmetric_adjacency(&[(1, 2), (2, 1), (2, 3)]);
        assert_eq!(adjacency.get(&1), Some(&vec![2]));
        assert_eq!(adjacency.get(&2), Some(&vec![1, 3]));
        assert_eq!(adjacency.get(&3), Some(&vec![2]));
    }

    #[test]
    fn extract_sde_version_stamp_finds_the_stamped_filename() {
        let html =
            r#"<a href="eve_3480926_20260826_134500.db.gz">eve_3480926_20260826_134500.db.gz</a>"#;
        assert_eq!(
            extract_sde_version_stamp(html),
            Some("eve_3480926_20260826_134500".to_string())
        );
    }

    #[test]
    fn extract_sde_version_stamp_returns_none_without_a_matching_pattern() {
        assert_eq!(
            extract_sde_version_stamp("<html>no match here</html>"),
            None
        );
    }
}
