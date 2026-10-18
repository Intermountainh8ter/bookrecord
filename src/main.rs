//! bookrecord: record L2 order books from exchange websocket streams.
//!
//! `record` keeps a local book in sync with the exchange's diff stream using
//! the exchange's own continuity rule (Binance: update-id chain; Kraken:
//! CRC32 checksum of the top ten levels; Coinbase: sequence numbers), writes
//! every raw event plus periodic book tops to a zstd-compressed JSON Lines
//! file, and measures the time from receiving a frame to having applied it.
//! A gap triggers a logged resync from a fresh snapshot; nothing is patched
//! silently. `verify` re-reads a capture and re-checks the continuity rule
//! independently of the live run, then reports gaps and latency percentiles.
//!
//! Only Binance is implemented in this version. Kraken and Coinbase adapters
//! are listed, not stubbed with fake output.

use clap::{Parser, Subcommand};
use futures_util::StreamExt;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Parser)]
#[command(name = "bookrecord", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Record one symbol's book from one venue for a fixed number of minutes.
    Record {
        /// binance | binance-us (kraken and coinbase are planned, not implemented)
        #[arg(long)]
        venue: String,
        /// Exchange symbol, e.g. BTCUSDT
        #[arg(long)]
        symbol: String,
        #[arg(long)]
        out_dir: PathBuf,
        #[arg(long, default_value_t = 5)]
        minutes: u64,
        /// Write a top-N book record every this many applied events.
        #[arg(long, default_value_t = 500)]
        top_every: u64,
        #[arg(long, default_value_t = 20)]
        top_levels: usize,
    },
    /// Re-check a capture's continuity and report gaps and latency percentiles.
    Verify {
        capture: PathBuf,
    },
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Rec {
    Meta { venue: String, symbol: String, started_unix_ns: u64, tool: String },
    Snapshot { recv_ns: u64, last_update_id: u64, bids: Vec<(String, String)>, asks: Vec<(String, String)> },
    Event { recv_ns: u64, apply_ns: u64, first_id: u64, last_id: u64, event_time_ms: u64, n_bids: usize, n_asks: usize, raw: serde_json::Value },
    Gap { recv_ns: u64, expected_first_id: u64, got_first_id: u64 },
    Top { recv_ns: u64, last_id: u64, bids: Vec<(String, String)>, asks: Vec<(String, String)> },
    End { recv_ns: u64, events: u64, gaps: u64, resyncs: u64, dropped_stale: u64 },
}

fn now_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}

#[derive(Default)]
struct Book {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
}

impl Book {
    fn load(&mut self, bids: &[(String, String)], asks: &[(String, String)]) -> anyhow::Result<()> {
        self.bids.clear();
        self.asks.clear();
        for (p, q) in bids {
            self.bids.insert(p.parse()?, q.parse()?);
        }
        for (p, q) in asks {
            self.asks.insert(p.parse()?, q.parse()?);
        }
        Ok(())
    }
    fn apply(&mut self, bids: &[(String, String)], asks: &[(String, String)]) -> anyhow::Result<()> {
        for (p, q) in bids {
            let (p, q): (Decimal, Decimal) = (p.parse()?, q.parse()?);
            if q.is_zero() { self.bids.remove(&p); } else { self.bids.insert(p, q); }
        }
        for (p, q) in asks {
            let (p, q): (Decimal, Decimal) = (p.parse()?, q.parse()?);
            if q.is_zero() { self.asks.remove(&p); } else { self.asks.insert(p, q); }
        }
        Ok(())
    }
    fn top(&self, n: usize) -> (Vec<(String, String)>, Vec<(String, String)>) {
        let bids = self.bids.iter().rev().take(n).map(|(p, q)| (p.to_string(), q.to_string())).collect();
        let asks = self.asks.iter().take(n).map(|(p, q)| (p.to_string(), q.to_string())).collect();
        (bids, asks)
    }
    fn crossed(&self) -> bool {
        match (self.bids.keys().next_back(), self.asks.keys().next()) {
            (Some(b), Some(a)) => b >= a,
            _ => false,
        }
    }
}

#[derive(Deserialize)]
struct BinanceSnapshot {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    bids: Vec<(String, String)>,
    asks: Vec<(String, String)>,
}

#[derive(Deserialize)]
struct BinanceDepthEvent {
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "U")]
    first_id: u64,
    #[serde(rename = "u")]
    last_id: u64,
    #[serde(rename = "b")]
    bids: Vec<(String, String)>,
    #[serde(rename = "a")]
    asks: Vec<(String, String)>,
}

/// Binance-family hosts share one API shape. Binance.com and its spot testnet answer HTTP 451
/// from the United States; Binance.US does not.
fn binance_hosts(venue: &str) -> Option<(&'static str, &'static str)> {
    match venue {
        "binance" => Some(("https://api.binance.com", "wss://stream.binance.com:9443")),
        "binance-us" => Some(("https://api.binance.us", "wss://stream.binance.us:9443")),
        _ => None,
    }
}

fn binance_snapshot(rest: &str, symbol: &str) -> anyhow::Result<BinanceSnapshot> {
    let url = format!("{rest}/api/v3/depth?symbol={symbol}&limit=1000");
    Ok(ureq::get(&url).timeout(Duration::from_secs(20)).call()?.into_json()?)
}

struct Writer {
    enc: zstd::Encoder<'static, std::fs::File>,
    lines: u64,
}

impl Writer {
    fn new(path: &PathBuf) -> anyhow::Result<Self> {
        let f = std::fs::File::create(path)?;
        Ok(Self { enc: zstd::Encoder::new(f, 3)?, lines: 0 })
    }
    fn write(&mut self, r: &Rec) -> anyhow::Result<()> {
        serde_json::to_writer(&mut self.enc, r)?;
        self.enc.write_all(b"\n")?;
        self.lines += 1;
        Ok(())
    }
    fn finish(self) -> anyhow::Result<()> {
        self.enc.finish()?;
        Ok(())
    }
}

async fn record_binance(venue: &str, symbol: String, out_dir: PathBuf, minutes: u64, top_every: u64, top_levels: usize) -> anyhow::Result<()> {
    let (rest, ws_base) = binance_hosts(venue).ok_or_else(|| anyhow::anyhow!("unknown binance venue {venue}"))?;
    let rest = rest.to_string();
    std::fs::create_dir_all(&out_dir)?;
    let started = now_ns();
    let stem = format!("{venue}-{}-{}", symbol.to_lowercase(), started / 1_000_000_000);
    let path = out_dir.join(format!("{stem}.jsonl.zst"));
    let mut w = Writer::new(&path)?;
    w.write(&Rec::Meta { venue: venue.to_string(), symbol: symbol.clone(), started_unix_ns: started, tool: format!("bookrecord {}", env!("CARGO_PKG_VERSION")) })?;

    let url = format!("{ws_base}/ws/{}@depth@100ms", symbol.to_lowercase());
    let (ws, _) = tokio_tungstenite::connect_async(&url).await?;
    let (_, mut rx) = ws.split();
    eprintln!("connected {url}");

    // Snapshot after the stream is open so buffered events bracket it.
    let (sym, r) = (symbol.clone(), rest.clone());
    let snap = tokio::task::spawn_blocking(move || binance_snapshot(&r, &sym)).await??;
    let snap_recv = now_ns();
    let mut book = Book::default();
    book.load(&snap.bids, &snap.asks)?;
    w.write(&Rec::Snapshot { recv_ns: snap_recv, last_update_id: snap.last_update_id, bids: snap.bids.clone(), asks: snap.asks.clone() })?;
    let mut last_id = snap.last_update_id;
    let mut synced = false;

    let deadline = Instant::now() + Duration::from_secs(minutes * 60);
    let (mut events, mut gaps, mut resyncs, mut dropped_stale) = (0u64, 0u64, 0u64, 0u64);
    let mut lat: Vec<u64> = Vec::new();

    loop {
        let msg = tokio::select! {
            m = rx.next() => m,
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => break,
            _ = tokio::signal::ctrl_c() => { eprintln!("interrupted"); break; }
        };
        let Some(msg) = msg else { eprintln!("stream closed"); break };
        let msg = msg?;
        let recv = now_ns();
        let t0 = Instant::now();
        let text = match msg {
            tokio_tungstenite::tungstenite::Message::Text(t) => t,
            tokio_tungstenite::tungstenite::Message::Ping(_) | tokio_tungstenite::tungstenite::Message::Pong(_) => continue,
            tokio_tungstenite::tungstenite::Message::Close(_) => break,
            _ => continue,
        };
        let raw: serde_json::Value = serde_json::from_str(&text)?;
        let ev: BinanceDepthEvent = serde_json::from_value(raw.clone())?;
        // Binance rule: drop events with u <= lastUpdateId; the first kept event must satisfy U <= lastUpdateId+1 <= u.
        if ev.last_id <= last_id {
            dropped_stale += 1;
            continue;
        }
        if !synced {
            if ev.first_id > last_id + 1 {
                // Snapshot is older than the buffered stream: resync.
                gaps += 1;
                w.write(&Rec::Gap { recv_ns: recv, expected_first_id: last_id + 1, got_first_id: ev.first_id })?;
                let (sym, r) = (symbol.clone(), rest.clone());
                let s = tokio::task::spawn_blocking(move || binance_snapshot(&r, &sym)).await??;
                book.load(&s.bids, &s.asks)?;
                w.write(&Rec::Snapshot { recv_ns: now_ns(), last_update_id: s.last_update_id, bids: s.bids.clone(), asks: s.asks.clone() })?;
                last_id = s.last_update_id;
                resyncs += 1;
                continue;
            }
            synced = true;
        } else if ev.first_id != last_id + 1 {
            gaps += 1;
            w.write(&Rec::Gap { recv_ns: recv, expected_first_id: last_id + 1, got_first_id: ev.first_id })?;
            let (sym, r) = (symbol.clone(), rest.clone());
            let s = tokio::task::spawn_blocking(move || binance_snapshot(&r, &sym)).await??;
            book.load(&s.bids, &s.asks)?;
            w.write(&Rec::Snapshot { recv_ns: now_ns(), last_update_id: s.last_update_id, bids: s.bids.clone(), asks: s.asks.clone() })?;
            last_id = s.last_update_id;
            resyncs += 1;
            synced = false;
            continue;
        }
        book.apply(&ev.bids, &ev.asks)?;
        anyhow::ensure!(!book.crossed(), "book crossed after event {}..{}", ev.first_id, ev.last_id);
        last_id = ev.last_id;
        let apply_ns = t0.elapsed().as_nanos() as u64;
        lat.push(apply_ns);
        events += 1;
        w.write(&Rec::Event { recv_ns: recv, apply_ns, first_id: ev.first_id, last_id: ev.last_id, event_time_ms: ev.event_time, n_bids: ev.bids.len(), n_asks: ev.asks.len(), raw })?;
        if events % top_every == 0 {
            let (b, a) = book.top(top_levels);
            w.write(&Rec::Top { recv_ns: now_ns(), last_id, bids: b, asks: a })?;
            eprintln!("{events} events, {gaps} gaps, best bid/ask {} / {}", book.bids.keys().next_back().map(|d| d.to_string()).unwrap_or_default(), book.asks.keys().next().map(|d| d.to_string()).unwrap_or_default());
        }
    }
    w.write(&Rec::End { recv_ns: now_ns(), events, gaps, resyncs, dropped_stale })?;
    let lines = w.lines;
    w.finish()?;

    lat.sort_unstable();
    let pct = |q: f64| -> u64 { if lat.is_empty() { 0 } else { lat[((lat.len() - 1) as f64 * q).round() as usize] } };
    let bytes = std::fs::read(&path)?;
    let sha = {
        use sha2::Digest;
        hex::encode(sha2::Sha256::digest(&bytes))
    };
    let manifest = serde_json::json!({
        "tool": format!("bookrecord {}", env!("CARGO_PKG_VERSION")),
        "venue": venue, "symbol": symbol, "file": path.file_name().and_then(|s| s.to_str()),
        "sha256": sha,
        "bytes": bytes.len(), "lines": lines, "started_unix_ns": started, "ended_unix_ns": now_ns(),
        "events": events, "gaps": gaps, "resyncs": resyncs, "dropped_stale": dropped_stale,
        "apply_latency_ns": { "p50": pct(0.5), "p90": pct(0.9), "p99": pct(0.99), "max": lat.last().copied().unwrap_or(0) },
        "continuity_rule": "Binance diff-depth: first event U <= lastUpdateId+1 <= u, then each U == previous u + 1; otherwise gap + resync from a fresh REST snapshot"
    });
    std::fs::write(out_dir.join(format!("{stem}.manifest.json")), serde_json::to_string_pretty(&manifest)?)?;
    println!("{}", serde_json::to_string_pretty(&manifest)?);
    Ok(())
}

fn verify(capture: &PathBuf) -> anyhow::Result<()> {
    let f = std::fs::File::open(capture)?;
    let dec = zstd::Decoder::new(f)?;
    let reader = std::io::BufReader::new(dec);
    // Three independent checks: (1) every applied event continues the chain from the record
    // before it (a break here means the recorder applied an event it should have rejected);
    // (2) every logged gap names the expected id the chain actually required; (3) the End
    // record's counts match what the file holds.
    let (mut events, mut gaps_logged, mut gaps_well_formed, mut chain_breaks, mut snapshots) = (0u64, 0u64, 0u64, 0u64, 0u64);
    let mut last_id: Option<u64> = None;
    let mut synced = false;
    let mut lat: Vec<u64> = Vec::new();
    let mut end: Option<serde_json::Value> = None;
    for line in reader.lines() {
        let r: Rec = serde_json::from_str(&line?)?;
        match r {
            Rec::Snapshot { last_update_id, .. } => { snapshots += 1; last_id = Some(last_update_id); synced = false; }
            Rec::Event { first_id, last_id: u, apply_ns, .. } => {
                if let Some(prev) = last_id {
                    let ok = if synced { first_id == prev + 1 } else { first_id <= prev + 1 && u >= prev + 1 };
                    if !ok { chain_breaks += 1; }
                }
                synced = true;
                last_id = Some(u);
                lat.push(apply_ns);
                events += 1;
            }
            Rec::Gap { expected_first_id, got_first_id, .. } => {
                gaps_logged += 1;
                if last_id.map_or(false, |p| expected_first_id == p + 1) && got_first_id > expected_first_id { gaps_well_formed += 1; }
            }
            Rec::End { events: e, gaps: g, resyncs, dropped_stale, .. } => end = Some(serde_json::json!({"events": e, "gaps": g, "resyncs": resyncs, "dropped_stale": dropped_stale})),
            _ => {}
        }
    }
    lat.sort_unstable();
    let pct = |q: f64| -> u64 { if lat.is_empty() { 0 } else { lat[((lat.len() - 1) as f64 * q).round() as usize] } };
    let end_ok = end.as_ref().map_or(false, |e| e["events"].as_u64() == Some(events) && e["gaps"].as_u64() == Some(gaps_logged));
    let report = serde_json::json!({
        "capture": capture.to_string_lossy(), "snapshots": snapshots, "events": events,
        "chain_breaks_in_applied_events": chain_breaks, "gaps_logged": gaps_logged, "gaps_well_formed": gaps_well_formed,
        "end_record_matches": end_ok,
        "consistent": chain_breaks == 0 && gaps_logged == gaps_well_formed && end_ok,
        "apply_latency_ns": { "p50": pct(0.5), "p90": pct(0.9), "p99": pct(0.99) }, "end_record": end
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    if report["consistent"] != true {
        std::process::exit(1);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Cmd::Record { venue, symbol, out_dir, minutes, top_every, top_levels } => match venue.as_str() {
            "binance" | "binance-us" => record_binance(&venue, symbol, out_dir, minutes, top_every, top_levels).await,
            "kraken" | "coinbase" => anyhow::bail!("{venue}: adapter not implemented yet (planned: Kraken v2 CRC32 checksum, Coinbase level2 sequence)"),
            other => anyhow::bail!("unknown venue {other}"),
        },
        Cmd::Verify { capture } => verify(&capture),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn book_applies_deletes_and_detects_crossing() {
        let mut b = Book::default();
        b.load(&[("100".into(), "1".into()), ("99".into(), "2".into())], &[("101".into(), "1".into())]).unwrap();
        b.apply(&[("100".into(), "0".into())], &[]).unwrap();
        assert_eq!(b.top(5).0.len(), 1);
        assert!(!b.crossed());
        b.apply(&[("102".into(), "1".into())], &[]).unwrap();
        assert!(b.crossed());
    }

    #[test]
    fn verify_counts_gaps_the_same_way_as_the_recorder() {
        let dir = std::env::temp_dir().join(format!("bookrecord-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("c.jsonl.zst");
        let mut w = Writer::new(&path).unwrap();
        w.write(&Rec::Snapshot { recv_ns: 0, last_update_id: 10, bids: vec![], asks: vec![] }).unwrap();
        w.write(&Rec::Event { recv_ns: 0, apply_ns: 5, first_id: 9, last_id: 12, event_time_ms: 0, n_bids: 0, n_asks: 0, raw: serde_json::Value::Null }).unwrap();
        w.write(&Rec::Event { recv_ns: 0, apply_ns: 7, first_id: 13, last_id: 13, event_time_ms: 0, n_bids: 0, n_asks: 0, raw: serde_json::Value::Null }).unwrap();
        w.write(&Rec::Gap { recv_ns: 0, expected_first_id: 14, got_first_id: 20 }).unwrap();
        w.write(&Rec::Snapshot { recv_ns: 0, last_update_id: 25, bids: vec![], asks: vec![] }).unwrap();
        w.write(&Rec::Event { recv_ns: 0, apply_ns: 9, first_id: 24, last_id: 26, event_time_ms: 0, n_bids: 0, n_asks: 0, raw: serde_json::Value::Null }).unwrap();
        w.write(&Rec::End { recv_ns: 0, events: 3, gaps: 1, resyncs: 1, dropped_stale: 0 }).unwrap();
        w.finish().unwrap();
        // verify exits the process on inconsistency; a consistent file returns Ok.
        verify(&path).unwrap();
    }
}
