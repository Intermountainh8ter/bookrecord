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
        /// binance | binance-us | kraken (symbol like BTC/USD) | coinbase (symbol like BTC-USD)
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
    /// Seeded synthetic experiment: detection rate of chain, snapshot and checksum checks on chain-preserving dropped-delete defects.
    Stalecheck {
        #[arg(long, default_value_t = 7)]
        seed: u64,
        #[arg(long, default_value_t = 200)]
        defects: u64,
        #[arg(long)]
        out: PathBuf,
    },
    /// Seeded synthetic experiment: detection of chain-preserving dropped-delete defects as a function of the defect's book depth rank (1-50).
    Depthcheck {
        #[arg(long, default_value_t = 7)]
        seed: u64,
        #[arg(long, default_value_t = 500)]
        trials: u64,
        #[arg(long)]
        out: PathBuf,
    },
    /// Seeded synthetic experiment: updates from a chain-preserving dropped-delete defect to its catch by a periodic full snapshot comparison, per cadence.
    Latencycheck {
        #[arg(long, default_value_t = 7)]
        seed: u64,
        #[arg(long, default_value_t = 500)]
        trials: u64,
        #[arg(long)]
        out: PathBuf,
    },
    /// Render `results/*.manifest.json` (+ matching `.verify.json`) into the README results block.
    Report {
        #[arg(long, default_value = "results")]
        dir: PathBuf,
        #[arg(long, default_value = "README.md")]
        readme: PathBuf,
    },
}

fn report(dir: &PathBuf, readme: &PathBuf) -> anyhow::Result<()> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.ends_with(".manifest.json")))
        .collect();
    files.sort();
    anyhow::ensure!(!files.is_empty(), "no *.manifest.json in {}", dir.display());
    let mut t = String::from("| venue | symbol | minutes | events | gaps | resyncs | apply p50 / p99 (µs) | verify consistent | capture sha256 |\n|---|---|---|---|---|---|---|---|---|\n");
    for f in &files {
        let m: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(f)?)?;
        let vpath = PathBuf::from(f.to_string_lossy().replace(".manifest.json", ".verify.json"));
        let consistent = match std::fs::read_to_string(&vpath) {
            Ok(s) => serde_json::from_str::<serde_json::Value>(&s)?["consistent"].as_bool().map(|b| b.to_string()).unwrap_or("?".into()),
            Err(_) => "no verify file".into(),
        };
        let g = |k: &str| m.get(k).ok_or_else(|| anyhow::anyhow!("{} missing {k}", f.display()));
        let minutes = (g("ended_unix_ns")?.as_u64().unwrap_or(0).saturating_sub(g("started_unix_ns")?.as_u64().unwrap_or(0))) as f64 / 60e9;
        let lat = g("apply_latency_ns")?;
        t.push_str(&format!(
            "| {} | {} | {:.1} | {} | {} | {} | {:.0} / {:.0} | {} | `{}` |\n",
            g("venue")?.as_str().unwrap_or("?"), g("symbol")?.as_str().unwrap_or("?"), minutes, g("events")?, g("gaps")?, g("resyncs")?,
            lat["p50"].as_f64().unwrap_or(0.0) / 1000.0, lat["p99"].as_f64().unwrap_or(0.0) / 1000.0, consistent,
            &g("sha256")?.as_str().unwrap_or("?")[..16]
        ));
    }
    t.push_str("\nRendered by `bookrecord report` from `results/*.manifest.json` and `results/*.verify.json`; capture files are not committed, their hashes are.\n");
    const START: &str = "<!-- RESULTS:START -->";
    const END: &str = "<!-- RESULTS:END -->";
    let text = std::fs::read_to_string(readme)?;
    let s = text.find(START).ok_or_else(|| anyhow::anyhow!("README missing {START}"))?;
    let e = text.find(END).ok_or_else(|| anyhow::anyhow!("README missing {END}"))?;
    anyhow::ensure!(s < e);
    std::fs::write(readme, format!("{}\n{}{}", &text[..s + START.len()], t, &text[e..]))?;
    print!("{t}");
    Ok(())
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


// ---------- Kraken v2 (CRC32 checksum) ----------

#[derive(Deserialize)]
struct KrakenPairInfo {
    pair_decimals: u32,
    lot_decimals: u32,
}

fn kraken_precision(symbol: &str) -> anyhow::Result<(u32, u32)> {
    // "BTC/USD" -> REST pair query "XBTUSD" works via the altname search.
    let alt = symbol.replace('/', "").replace("BTC", "XBT");
    let v: serde_json::Value = ureq::get(&format!("https://api.kraken.com/0/public/AssetPairs?pair={alt}")).timeout(Duration::from_secs(20)).call()?.into_json()?;
    let result = v["result"].as_object().ok_or_else(|| anyhow::anyhow!("AssetPairs: {}", v["error"]))?;
    let (_, info) = result.iter().next().ok_or_else(|| anyhow::anyhow!("AssetPairs: no pair for {symbol}"))?;
    let info: KrakenPairInfo = serde_json::from_value(info.clone())?;
    Ok((info.pair_decimals, info.lot_decimals))
}

/// Kraken's book checksum: for the top ten asks (ascending) then top ten bids (descending),
/// format price and qty with the pair's decimals, drop the decimal point and leading zeros,
/// concatenate everything, CRC32.
fn kraken_checksum(book: &Book, price_dec: u32, qty_dec: u32) -> u32 {
    kraken_checksum_depth(book, price_dec, qty_dec, 10)
}

fn kraken_checksum_depth(book: &Book, price_dec: u32, qty_dec: u32, depth: usize) -> u32 {
    let fmt = |d: &Decimal, dec: u32| -> String {
        let s = format!("{:.*}", dec as usize, d);
        let s = s.replace('.', "");
        let t = s.trim_start_matches('0');
        if t.is_empty() { "0".to_string() } else { t.to_string() }
    };
    let mut buf = String::new();
    for (p, q) in book.asks.iter().take(depth) {
        buf.push_str(&fmt(p, price_dec));
        buf.push_str(&fmt(q, qty_dec));
    }
    for (p, q) in book.bids.iter().rev().take(depth) {
        buf.push_str(&fmt(p, price_dec));
        buf.push_str(&fmt(q, qty_dec));
    }
    crc32fast::hash(buf.as_bytes())
}

fn kraken_levels(v: &serde_json::Value) -> Vec<(String, String)> {
    v.as_array().map(|a| a.iter().map(|l| (l["price"].to_string(), l["qty"].to_string())).collect()).unwrap_or_default()
}

async fn record_kraken(symbol: String, out_dir: PathBuf, minutes: u64, top_every: u64, top_levels: usize) -> anyhow::Result<()> {
    std::fs::create_dir_all(&out_dir)?;
    let (price_dec, qty_dec) = kraken_precision(&symbol)?;
    let started = now_ns();
    let stem = format!("kraken-{}-{}", symbol.to_lowercase().replace('/', ""), started / 1_000_000_000);
    let path = out_dir.join(format!("{stem}.jsonl.zst"));
    let mut w = Writer::new(&path)?;
    w.write(&Rec::Meta { venue: "kraken".into(), symbol: symbol.clone(), started_unix_ns: started, tool: format!("bookrecord {}", env!("CARGO_PKG_VERSION")) })?;

    let (ws, _) = tokio_tungstenite::connect_async("wss://ws.kraken.com/v2").await?;
    let (mut tx, mut rx) = ws.split();
    use futures_util::SinkExt;
    let sub = serde_json::json!({"method": "subscribe", "params": {"channel": "book", "symbol": [symbol], "depth": 10, "snapshot": true}});
    tx.send(tokio_tungstenite::tungstenite::Message::Text(sub.to_string().into())).await?;
    eprintln!("connected wss://ws.kraken.com/v2, depth 10, precision {price_dec}/{qty_dec}");

    let mut book = Book::default();
    let deadline = Instant::now() + Duration::from_secs(minutes * 60);
    let (mut events, mut gaps, mut resyncs, mut snapshots) = (0u64, 0u64, 0u64, 0u64);
    let mut lat: Vec<u64> = Vec::new();
    let mut seq: u64 = 0;
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
            tokio_tungstenite::tungstenite::Message::Close(_) => break,
            _ => continue,
        };
        let raw: serde_json::Value = serde_json::from_str(&text)?;
        if raw["channel"] != "book" { continue; }
        let kind = raw["type"].as_str().unwrap_or("");
        let data = &raw["data"][0];
        let bids = kraken_levels(&data["bids"]);
        let asks = kraken_levels(&data["asks"]);
        let expected = data["checksum"].as_u64().unwrap_or(0) as u32;
        if kind == "snapshot" {
            book.load(&bids, &asks)?;
            snapshots += 1;
            w.write(&Rec::Snapshot { recv_ns: recv, last_update_id: seq, bids: bids.clone(), asks: asks.clone() })?;
            continue;
        }
        book.apply(&bids, &asks)?;
        // Kraken keeps the book at the subscribed depth: trim anything beyond 10 levels.
        while book.asks.len() > 10 { let k = *book.asks.keys().next_back().unwrap(); book.asks.remove(&k); }
        while book.bids.len() > 10 { let k = *book.bids.keys().next().unwrap(); book.bids.remove(&k); }
        let got = kraken_checksum(&book, price_dec, qty_dec);
        seq += 1;
        if got != expected {
            gaps += 1;
            w.write(&Rec::Gap { recv_ns: recv, expected_first_id: expected as u64, got_first_id: got as u64 })?;
            // Resync: unsubscribe and resubscribe for a fresh snapshot.
            let unsub = serde_json::json!({"method": "unsubscribe", "params": {"channel": "book", "symbol": [symbol.clone()], "depth": 10}});
            tx.send(tokio_tungstenite::tungstenite::Message::Text(unsub.to_string().into())).await?;
            tx.send(tokio_tungstenite::tungstenite::Message::Text(sub.to_string().into())).await?;
            resyncs += 1;
            continue;
        }
        anyhow::ensure!(!book.crossed(), "book crossed at update {seq}");
        let apply_ns = t0.elapsed().as_nanos() as u64;
        lat.push(apply_ns);
        events += 1;
        w.write(&Rec::Event { recv_ns: recv, apply_ns, first_id: seq, last_id: seq, event_time_ms: 0, n_bids: bids.len(), n_asks: asks.len(), raw })?;
        if events % top_every == 0 {
            let (b, a) = book.top(top_levels);
            w.write(&Rec::Top { recv_ns: now_ns(), last_id: seq, bids: b, asks: a })?;
            eprintln!("{events} updates, {gaps} checksum mismatches");
        }
    }
    w.write(&Rec::End { recv_ns: now_ns(), events, gaps, resyncs, dropped_stale: 0 })?;
    let lines = w.lines;
    w.finish()?;
    lat.sort_unstable();
    let pct = |q: f64| -> u64 { if lat.is_empty() { 0 } else { lat[((lat.len() - 1) as f64 * q).round() as usize] } };
    let bytes = std::fs::read(&path)?;
    let sha = { use sha2::Digest; hex::encode(sha2::Sha256::digest(&bytes)) };
    let manifest = serde_json::json!({
        "tool": format!("bookrecord {}", env!("CARGO_PKG_VERSION")),
        "venue": "kraken", "symbol": symbol, "file": path.file_name().and_then(|s| s.to_str()), "sha256": sha,
        "bytes": bytes.len(), "lines": lines, "started_unix_ns": started, "ended_unix_ns": now_ns(),
        "events": events, "gaps": gaps, "resyncs": resyncs, "dropped_stale": 0, "snapshots": snapshots,
        "apply_latency_ns": { "p50": pct(0.5), "p90": pct(0.9), "p99": pct(0.99), "max": lat.last().copied().unwrap_or(0) },
        "continuity_rule": "Kraken v2 book: CRC32 of the top-10 asks then bids (price and qty at pair precision, decimal point and leading zeros removed) must equal the message checksum; a mismatch is a gap and triggers resubscribe"
    });
    std::fs::write(out_dir.join(format!("{stem}.manifest.json")), serde_json::to_string_pretty(&manifest)?)?;
    println!("{}", serde_json::to_string_pretty(&manifest)?);
    Ok(())
}

// ---------- Coinbase Advanced Trade (level2, per-connection sequence numbers) ----------

/// Coinbase's level2 channel sends a `snapshot` event on subscribe and `update` events after it,
/// every message carrying a `sequence_num` that increments by one per message on the connection
/// (across channels, including the subscription acknowledgement). A skipped number is a dropped
/// message; the documented recovery is to resubscribe for a fresh snapshot. Sides are `bid` and
/// `offer`; a `new_quantity` of 0 removes the level.
fn coinbase_levels(updates: &serde_json::Value) -> (Vec<(String, String)>, Vec<(String, String)>) {
    let (mut bids, mut asks) = (Vec::new(), Vec::new());
    if let Some(a) = updates.as_array() {
        for u in a {
            let px = u["price_level"].as_str().unwrap_or("0").to_string();
            let qty = u["new_quantity"].as_str().unwrap_or("0").to_string();
            match u["side"].as_str() {
                Some("bid") => bids.push((px, qty)),
                Some("offer") | Some("ask") => asks.push((px, qty)),
                _ => {}
            }
        }
    }
    (bids, asks)
}

async fn record_coinbase(symbol: String, out_dir: PathBuf, minutes: u64, top_every: u64, top_levels: usize) -> anyhow::Result<()> {
    std::fs::create_dir_all(&out_dir)?;
    let started = now_ns();
    let stem = format!("coinbase-{}-{}", symbol.to_lowercase().replace('-', ""), started / 1_000_000_000);
    let path = out_dir.join(format!("{stem}.jsonl.zst"));
    let mut w = Writer::new(&path)?;
    w.write(&Rec::Meta { venue: "coinbase".into(), symbol: symbol.clone(), started_unix_ns: started, tool: format!("bookrecord {}", env!("CARGO_PKG_VERSION")) })?;

    let url = "wss://advanced-trade-ws.coinbase.com";
    let (ws, _) = tokio_tungstenite::connect_async(url).await?;
    let (mut tx, mut rx) = ws.split();
    use futures_util::SinkExt;
    let sub = serde_json::json!({"type": "subscribe", "product_ids": [symbol], "channel": "level2"});
    let unsub = serde_json::json!({"type": "unsubscribe", "product_ids": [symbol], "channel": "level2"});
    tx.send(tokio_tungstenite::tungstenite::Message::Text(sub.to_string().into())).await?;
    eprintln!("connected {url}, level2 {symbol}");

    let mut book = Book::default();
    let deadline = Instant::now() + Duration::from_secs(minutes * 60);
    let (mut events, mut gaps, mut resyncs, mut snapshots, mut skipped_resync) = (0u64, 0u64, 0u64, 0u64, 0u64);
    let mut lat: Vec<u64> = Vec::new();
    // `last_seq` is the sequence number of the last message in the recorded chain; `synced` is
    // false between a gap and the next snapshot, when messages are skipped rather than recorded.
    let mut last_seq: Option<u64> = None;
    let mut synced = false;
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
            tokio_tungstenite::tungstenite::Message::Close(_) => break,
            _ => continue,
        };
        let raw: serde_json::Value = serde_json::from_str(&text)?;
        if raw["type"] == "error" {
            anyhow::bail!("coinbase error message: {}", raw["message"]);
        }
        let Some(seq) = raw["sequence_num"].as_u64() else { continue };
        let channel = raw["channel"].as_str().unwrap_or("");
        let is_l2 = channel == "l2_data";
        let ev = if is_l2 { &raw["events"][0] } else { &serde_json::Value::Null };
        let kind = ev["type"].as_str().unwrap_or("");

        if is_l2 && kind == "snapshot" {
            let (bids, asks) = coinbase_levels(&ev["updates"]);
            book.load(&bids, &asks)?;
            snapshots += 1;
            w.write(&Rec::Snapshot { recv_ns: recv, last_update_id: seq, bids, asks })?;
            last_seq = Some(seq);
            synced = true;
            continue;
        }
        if !synced {
            // Before the first snapshot, or between a gap and the resubscribed snapshot.
            if last_seq.is_some() { skipped_resync += 1; }
            continue;
        }
        if let Some(prev) = last_seq {
            if seq != prev + 1 {
                gaps += 1;
                w.write(&Rec::Gap { recv_ns: recv, expected_first_id: prev + 1, got_first_id: seq })?;
                tx.send(tokio_tungstenite::tungstenite::Message::Text(unsub.to_string().into())).await?;
                tx.send(tokio_tungstenite::tungstenite::Message::Text(sub.to_string().into())).await?;
                resyncs += 1;
                synced = false;
                continue;
            }
        }
        let (bids, asks) = if is_l2 { coinbase_levels(&ev["updates"]) } else { (Vec::new(), Vec::new()) };
        if is_l2 {
            book.apply(&bids, &asks)?;
            anyhow::ensure!(!book.crossed(), "book crossed at sequence {seq}");
        }
        last_seq = Some(seq);
        let apply_ns = t0.elapsed().as_nanos() as u64;
        lat.push(apply_ns);
        events += 1;
        let event_time_ms = raw["timestamp"].as_str().and_then(rfc3339_to_ms).unwrap_or(0);
        w.write(&Rec::Event { recv_ns: recv, apply_ns, first_id: seq, last_id: seq, event_time_ms, n_bids: bids.len(), n_asks: asks.len(), raw })?;
        if events % top_every == 0 {
            let (b, a) = book.top(top_levels);
            w.write(&Rec::Top { recv_ns: now_ns(), last_id: seq, bids: b, asks: a })?;
            eprintln!("{events} messages, {gaps} sequence gaps, best bid/ask {} / {}", book.bids.keys().next_back().map(|d| d.to_string()).unwrap_or_default(), book.asks.keys().next().map(|d| d.to_string()).unwrap_or_default());
        }
    }
    w.write(&Rec::End { recv_ns: now_ns(), events, gaps, resyncs, dropped_stale: skipped_resync })?;
    let lines = w.lines;
    w.finish()?;
    lat.sort_unstable();
    let pct = |q: f64| -> u64 { if lat.is_empty() { 0 } else { lat[((lat.len() - 1) as f64 * q).round() as usize] } };
    let bytes = std::fs::read(&path)?;
    let sha = { use sha2::Digest; hex::encode(sha2::Sha256::digest(&bytes)) };
    let manifest = serde_json::json!({
        "tool": format!("bookrecord {}", env!("CARGO_PKG_VERSION")),
        "venue": "coinbase", "symbol": symbol, "file": path.file_name().and_then(|s| s.to_str()), "sha256": sha,
        "bytes": bytes.len(), "lines": lines, "started_unix_ns": started, "ended_unix_ns": now_ns(),
        "events": events, "gaps": gaps, "resyncs": resyncs, "dropped_stale": skipped_resync, "snapshots": snapshots,
        "apply_latency_ns": { "p50": pct(0.5), "p90": pct(0.9), "p99": pct(0.99), "max": lat.last().copied().unwrap_or(0) },
        "continuity_rule": "Coinbase Advanced Trade level2: every message's sequence_num must equal the previous message's plus one (the chain includes non-book messages on the connection); a skipped number is a gap, triggers unsubscribe/resubscribe, and messages are skipped (counted as dropped_stale) until the fresh snapshot"
    });
    std::fs::write(out_dir.join(format!("{stem}.manifest.json")), serde_json::to_string_pretty(&manifest)?)?;
    println!("{}", serde_json::to_string_pretty(&manifest)?);
    Ok(())
}

/// Parse an RFC 3339 UTC timestamp with optional fractional seconds into Unix milliseconds
/// without a date crate: `2026-10-19T15:04:05.123456Z`. Anything else returns None.
fn rfc3339_to_ms(s: &str) -> Option<u64> {
    let s = s.strip_suffix('Z')?;
    let (date, time) = s.split_once('T')?;
    let mut d = date.split('-');
    let (y, m, day): (i64, i64, i64) = (d.next()?.parse().ok()?, d.next()?.parse().ok()?, d.next()?.parse().ok()?);
    let (hms, frac) = match time.split_once('.') { Some((a, f)) => (a, f), None => (time, "") };
    let mut t = hms.split(':');
    let (h, mi, sec): (u64, u64, u64) = (t.next()?.parse().ok()?, t.next()?.parse().ok()?, t.next()?.parse().ok()?);
    let ms: u64 = if frac.is_empty() { 0 } else { format!("{:0<3}", &frac[..frac.len().min(3)]).parse().ok()? };
    // Days from civil date (Howard Hinnant's algorithm).
    let (y, m) = if m <= 2 { (y - 1, m + 9) } else { (y, m - 3) };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * m + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    Some((days as u64 * 86400 + h * 3600 + mi * 60 + sec) * 1000 + ms)
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

// ---- stalecheck: synthetic dropped-delete defects that keep the update-ID chain intact ----

struct SplitMix(u64);
impl SplitMix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 { self.next() % n }
}

const STALE_EVENTS: u64 = 1500;
const STALE_SNAPSHOT_EVERY: u64 = 500;
const STALE_WINDOW: u64 = 1000;
const STALE_MID: u64 = 100_000;

fn stale_price(bid: bool, off: u64) -> String {
    if bid { (STALE_MID - 1 - off).to_string() } else { (STALE_MID + 1 + off).to_string() }
}

/// Binance rule for synced events: first_id must equal previous last_id + 1.
fn chain_ok(prev_last: u64, first_id: u64) -> bool { first_id == prev_last + 1 }

#[derive(Default, Clone, Copy)]
struct StaleTrial { chain: bool, snapshot: bool, crc: bool, healed_undetected: bool }

fn stale_trial(rng: &mut SplitMix) -> StaleTrial {
    let mut truth = Book::default();
    let mut local = Book::default();
    let init = |rng: &mut SplitMix, bid: bool| -> Vec<(String, String)> {
        let mut offs = std::collections::BTreeSet::new();
        while offs.len() < 500 { offs.insert(rng.below(STALE_WINDOW)); }
        offs.into_iter().map(|o| (stale_price(bid, o), format!("{}.00000000", 1 + rng.below(9)))).collect()
    };
    let (b, a) = (init(rng, true), init(rng, false));
    truth.load(&b, &a).unwrap();
    local.load(&b, &a).unwrap();
    let inject_at = 100 + rng.below(900);
    let mut t = StaleTrial::default();
    let mut injected = false;
    let mut diverged = false;
    let mut last_id = 1000u64;
    for i in 1..=STALE_EVENTS {
        let first_id = last_id + 1;
        last_id = first_id + rng.below(3);
        if i > 1 && !chain_ok(first_id - 1, first_id) && !t.chain { t.chain = injected; }
        let bid = rng.below(2) == 0;
        let mut ch_t: Vec<(String, String)> = Vec::new();
        let mut ch_l: Vec<(String, String)> = Vec::new();
        if i == inject_at {
            // Delete one of the ten best levels in truth; the local copy never sees the delete.
            let k = rng.below(10) as usize;
            let price = if bid { truth.bids.keys().rev().nth(k).copied() } else { truth.asks.keys().nth(k).copied() }.unwrap();
            ch_t.push((price.to_string(), "0".into()));
            injected = true;
        } else {
            for _ in 0..(1 + rng.below(2)) {
                let off = rng.below(STALE_WINDOW);
                let p = stale_price(bid, off);
                let pd: Decimal = p.parse().unwrap();
                let present = if bid { truth.bids.contains_key(&pd) } else { truth.asks.contains_key(&pd) };
                let len = if bid { truth.bids.len() } else { truth.asks.len() };
                let q = if present && len > 100 && rng.below(2) == 0 { "0".to_string() } else { format!("{}.00000000", 1 + rng.below(9)) };
                ch_t.push((p.clone(), q.clone()));
                ch_l.push((p, q));
            }
        }
        let (bt, at) = if bid { (ch_t.as_slice(), &[][..]) } else { (&[][..], ch_t.as_slice()) };
        let (bl, al) = if bid { (ch_l.as_slice(), &[][..]) } else { (&[][..], ch_l.as_slice()) };
        truth.apply(bt, at).unwrap();
        local.apply(bl, al).unwrap();
        if injected {
            let same = truth.bids == local.bids && truth.asks == local.asks;
            if !same { diverged = true; }
            if diverged && same && !t.crc && !t.snapshot { t.healed_undetected = true; }
            if kraken_checksum(&truth, 2, 8) != kraken_checksum(&local, 2, 8) { t.crc = true; }
            if i % STALE_SNAPSHOT_EVERY == 0 && !same { t.snapshot = true; }
        }
    }
    t
}

fn stalecheck(seed: u64, defects: u64, out: &PathBuf) -> anyhow::Result<()> {
    let started_at = {
        let secs = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0) as i64;
        let (days, rem) = (secs.div_euclid(86400), secs.rem_euclid(86400));
        // civil-from-days (Howard Hinnant)
        let z = days + 719468;
        let era = z.div_euclid(146097);
        let doe = z.rem_euclid(146097);
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
        format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z", rem / 3600, rem % 3600 / 60, rem % 60)
    };
    let t0 = Instant::now();
    let mut rng = SplitMix(seed);
    let (mut chain, mut snap, mut crc, mut healed) = (0u64, 0u64, 0u64, 0u64);
    for _ in 0..defects {
        let t = stale_trial(&mut rng);
        chain += t.chain as u64;
        snap += t.snapshot as u64;
        crc += t.crc as u64;
        healed += t.healed_undetected as u64;
    }
    let rate = |n: u64| if defects == 0 { 0.0 } else { n as f64 / defects as f64 };
    let res = serde_json::json!({
        "experiment": "stalecheck",
        "params": { "seed": seed, "defects": defects, "events_per_trial": STALE_EVENTS, "snapshot_every": STALE_SNAPSHOT_EVERY,
                    "levels_per_side_initial": 500, "price_window_ticks": STALE_WINDOW, "defect_depth": "uniform over the best ten levels of a random side",
                    "crc_levels": 10, "note": "one defect per independent trial; the chain check cannot fire by construction because the defect changes no update ID" },
        "detection": {
            "defects": defects,
            "detected": { "update_id_chain": chain, "snapshot_every_500": snap, "crc32_top10": crc },
            "rate_by_check": { "update_id_chain": rate(chain), "snapshot_every_500": rate(snap), "crc32_top10": rate(crc) },
            "healed_before_any_detection": healed
        },
        "provenance": {
            "started_at": started_at, "seeds": [seed],
            "command": format!("cargo run --locked -q --release -- stalecheck --seed {seed} --defects {defects} --out {}", out.display()),
            "hardware": format!("cpu-only, {} {}", std::env::consts::OS, std::env::consts::ARCH),
            "wall_s": t0.elapsed().as_secs_f64().round(),
            "data_sources": [
                { "url": "in-process seeded synthetic stream (no external data)", "license": "n/a, generated by the code" },
                { "url": "https://developers.binance.com/docs/derivatives/options-trading/websocket-market-streams/How-to-manage-a-local-order-book-correctly", "license": "public vendor documentation, rule description only" }
            ]
        }
    });
    if let Some(dir) = out.parent() { std::fs::create_dir_all(dir)?; }
    std::fs::write(out, serde_json::to_string_pretty(&res)? + "\n")?;
    println!("{}", serde_json::to_string_pretty(&res["detection"])?);
    Ok(())
}

// ---- depthcheck: dropped-delete defect at a chosen depth rank, checksum depth 10 versus 25 ----

const DEPTH_MAX_RANK: u64 = 50;

#[derive(Default, Clone, Copy)]
struct DepthTrial { rank: u64, chain: bool, snapshot: bool, crc10: bool, crc25: bool }

/// One trial: the delete at `rank` (1 = best level) of a random side is applied to truth only.
fn depth_trial(rng: &mut SplitMix) -> DepthTrial {
    let mut truth = Book::default();
    let mut local = Book::default();
    let init = |rng: &mut SplitMix, bid: bool| -> Vec<(String, String)> {
        let mut offs = std::collections::BTreeSet::new();
        while offs.len() < 500 { offs.insert(rng.below(STALE_WINDOW)); }
        offs.into_iter().map(|o| (stale_price(bid, o), format!("{}.00000000", 1 + rng.below(9)))).collect()
    };
    let (b, a) = (init(rng, true), init(rng, false));
    truth.load(&b, &a).unwrap();
    local.load(&b, &a).unwrap();
    let rank = 1 + rng.below(DEPTH_MAX_RANK);
    let inject_at = 100 + rng.below(900);
    let mut t = DepthTrial { rank, ..Default::default() };
    let mut injected = false;
    let mut last_id = 1000u64;
    for i in 1..=STALE_EVENTS {
        let first_id = last_id + 1;
        last_id = first_id + rng.below(3);
        if injected && !chain_ok(first_id - 1, first_id) { t.chain = true; }
        let bid = rng.below(2) == 0;
        let mut ch_t: Vec<(String, String)> = Vec::new();
        let mut ch_l: Vec<(String, String)> = Vec::new();
        if i == inject_at {
            let k = (rank - 1) as usize;
            let price = if bid { truth.bids.keys().rev().nth(k).copied() } else { truth.asks.keys().nth(k).copied() }.unwrap();
            ch_t.push((price.to_string(), "0".into()));
            injected = true;
        } else {
            for _ in 0..(1 + rng.below(2)) {
                let off = rng.below(STALE_WINDOW);
                let p = stale_price(bid, off);
                let pd: Decimal = p.parse().unwrap();
                let present = if bid { truth.bids.contains_key(&pd) } else { truth.asks.contains_key(&pd) };
                let len = if bid { truth.bids.len() } else { truth.asks.len() };
                let q = if present && len > 100 && rng.below(2) == 0 { "0".to_string() } else { format!("{}.00000000", 1 + rng.below(9)) };
                ch_t.push((p.clone(), q.clone()));
                ch_l.push((p, q));
            }
        }
        let (bt, at) = if bid { (ch_t.as_slice(), &[][..]) } else { (&[][..], ch_t.as_slice()) };
        let (bl, al) = if bid { (ch_l.as_slice(), &[][..]) } else { (&[][..], ch_l.as_slice()) };
        truth.apply(bt, at).unwrap();
        local.apply(bl, al).unwrap();
        if injected {
            let same = truth.bids == local.bids && truth.asks == local.asks;
            if kraken_checksum_depth(&truth, 2, 8, 10) != kraken_checksum_depth(&local, 2, 8, 10) { t.crc10 = true; }
            if kraken_checksum_depth(&truth, 2, 8, 25) != kraken_checksum_depth(&local, 2, 8, 25) { t.crc25 = true; }
            if i % STALE_SNAPSHOT_EVERY == 0 && !same { t.snapshot = true; }
        }
    }
    t
}

fn utc_now() -> String {
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0) as i64;
    let (days, rem) = (secs.div_euclid(86400), secs.rem_euclid(86400));
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z", rem / 3600, rem % 3600 / 60, rem % 60)
}

fn depthcheck(seed: u64, trials: u64, out: &PathBuf) -> anyhow::Result<()> {
    let started_at = utc_now();
    let t0 = Instant::now();
    let mut rng = SplitMix(seed);
    let (mut chain, mut snap, mut c10, mut c25) = (0u64, 0u64, 0u64, 0u64);
    // index 0 = rank 1
    let mut inj = vec![0u64; DEPTH_MAX_RANK as usize];
    let mut hit10 = vec![0u64; DEPTH_MAX_RANK as usize];
    let mut hit25 = vec![0u64; DEPTH_MAX_RANK as usize];
    let (mut c10_in, mut c10_out, mut c25_in, mut c25_out) = (0u64, 0u64, 0u64, 0u64);
    for _ in 0..trials {
        let t = depth_trial(&mut rng);
        let r = (t.rank - 1) as usize;
        inj[r] += 1;
        chain += t.chain as u64;
        snap += t.snapshot as u64;
        c10 += t.crc10 as u64;
        c25 += t.crc25 as u64;
        hit10[r] += t.crc10 as u64;
        hit25[r] += t.crc25 as u64;
        if t.rank <= 10 { c10_in += t.crc10 as u64 } else { c10_out += t.crc10 as u64 }
        if t.rank <= 25 { c25_in += t.crc25 as u64 } else { c25_out += t.crc25 as u64 }
    }
    let n_le = |k: u64| inj.iter().take(k as usize).sum::<u64>();
    let rate = |n: u64| if trials == 0 { 0.0 } else { n as f64 / trials as f64 };
    let res = serde_json::json!({
        "experiment": "depthcheck",
        "params": { "seed": seed, "trials": trials, "events_per_trial": STALE_EVENTS, "snapshot_every": STALE_SNAPSHOT_EVERY,
                    "levels_per_side_initial": 500, "price_window_ticks": STALE_WINDOW, "defect_depth": "uniform over ranks 1-50 of a random side",
                    "note": "one defect per independent trial; the chain check cannot fire by construction because the defect changes no update ID" },
        "detection": {
            "trials": trials,
            "catch_count_by_depth": { "update_id_chain": chain, "snapshot_every_500": snap, "crc32_top10": c10, "crc32_top25": c25 },
            "rate_by_check": { "update_id_chain": rate(chain), "snapshot_every_500": rate(snap), "crc32_top10": rate(c10), "crc32_top25": rate(c25) },
            "injected_at_rank_le_10": n_le(10),
            "injected_at_rank_le_25": n_le(25),
            "crc32_top10_catches_rank_le_10": c10_in,
            "crc32_top10_catches_rank_gt_10": c10_out,
            "crc32_top25_catches_rank_le_25": c25_in,
            "crc32_top25_catches_rank_gt_25": c25_out,
            "injected_by_rank": inj,
            "crc32_top10_catches_by_rank": hit10,
            "crc32_top25_catches_by_rank": hit25
        },
        "provenance": {
            "started_at": started_at, "seeds": [seed],
            "command": format!("cargo run --locked -q --release -- depthcheck --seed {seed} --trials {trials} --out {}", out.display()),
            "hardware": format!("cpu-only, {} {}", std::env::consts::OS, std::env::consts::ARCH),
            "wall_s": t0.elapsed().as_secs_f64().round(),
            "data_sources": [
                { "url": "in-process seeded synthetic stream (no external data)", "license": "n/a, generated by the code" },
                { "url": "https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2.md", "license": "vendor documentation, read only for checksum depth (top 10)" },
                { "url": "https://docs.bitfinex.com/docs/ws-websocket-checksum", "license": "vendor documentation, read only for checksum depth (top 25)" }
            ]
        }
    });
    if let Some(dir) = out.parent() { std::fs::create_dir_all(dir)?; }
    std::fs::write(out, serde_json::to_string_pretty(&res)? + "\n")?;
    println!("{}", serde_json::to_string_pretty(&res["detection"]["catch_count_by_depth"])?);
    Ok(())
}

// ---- latencycheck: updates from injection to catch by a periodic full snapshot comparison ----

const LAT_EVENTS: u64 = 4000;
const LAT_CADENCES: [u64; 5] = [100, 250, 500, 1000, 2000];

/// One trial: returns, per cadence, `Some(updates since injection)` at the first comparison that finds a divergence.
fn latency_trial(rng: &mut SplitMix) -> [Option<u64>; 5] {
    let mut truth = Book::default();
    let mut local = Book::default();
    let init = |rng: &mut SplitMix, bid: bool| -> Vec<(String, String)> {
        let mut offs = std::collections::BTreeSet::new();
        while offs.len() < 500 { offs.insert(rng.below(STALE_WINDOW)); }
        offs.into_iter().map(|o| (stale_price(bid, o), format!("{}.00000000", 1 + rng.below(9)))).collect()
    };
    let (b, a) = (init(rng, true), init(rng, false));
    truth.load(&b, &a).unwrap();
    local.load(&b, &a).unwrap();
    let rank = 1 + rng.below(DEPTH_MAX_RANK);
    let inject_at = 100 + rng.below(900);
    let mut res = [None; 5];
    let mut injected = false;
    for i in 1..=LAT_EVENTS {
        let bid = rng.below(2) == 0;
        let mut ch_t: Vec<(String, String)> = Vec::new();
        let mut ch_l: Vec<(String, String)> = Vec::new();
        if i == inject_at {
            let k = (rank - 1) as usize;
            let price = if bid { truth.bids.keys().rev().nth(k).copied() } else { truth.asks.keys().nth(k).copied() }.unwrap();
            ch_t.push((price.to_string(), "0".into()));
            injected = true;
        } else {
            for _ in 0..(1 + rng.below(2)) {
                let off = rng.below(STALE_WINDOW);
                let p = stale_price(bid, off);
                let pd: Decimal = p.parse().unwrap();
                let present = if bid { truth.bids.contains_key(&pd) } else { truth.asks.contains_key(&pd) };
                let len = if bid { truth.bids.len() } else { truth.asks.len() };
                let q = if present && len > 100 && rng.below(2) == 0 { "0".to_string() } else { format!("{}.00000000", 1 + rng.below(9)) };
                ch_t.push((p.clone(), q.clone()));
                ch_l.push((p, q));
            }
        }
        let (bt, at) = if bid { (ch_t.as_slice(), &[][..]) } else { (&[][..], ch_t.as_slice()) };
        let (bl, al) = if bid { (ch_l.as_slice(), &[][..]) } else { (&[][..], ch_l.as_slice()) };
        truth.apply(bt, at).unwrap();
        local.apply(bl, al).unwrap();
        if injected {
            let same = truth.bids == local.bids && truth.asks == local.asks;
            for (c, n) in LAT_CADENCES.iter().enumerate() {
                if res[c].is_none() && i % n == 0 && !same { res[c] = Some(i - inject_at); }
            }
        }
    }
    res
}

fn p95(sorted: &[u64]) -> Option<u64> {
    if sorted.is_empty() { return None; }
    // nearest-rank: ceil(0.95 n)-th smallest
    let idx = (sorted.len() * 95).div_ceil(100).max(1) - 1;
    Some(sorted[idx])
}

fn latencycheck(seed: u64, trials: u64, out: &PathBuf) -> anyhow::Result<()> {
    let started_at = utc_now();
    let t0 = Instant::now();
    let mut rng = SplitMix(seed);
    let mut caught: Vec<Vec<u64>> = vec![Vec::new(); LAT_CADENCES.len()];
    for _ in 0..trials {
        let r = latency_trial(&mut rng);
        for (c, v) in r.iter().enumerate() {
            if let Some(u) = v { caught[c].push(*u); }
        }
    }
    let mut p95m = serde_json::Map::new();
    let mut maxm = serde_json::Map::new();
    let mut caughtm = serde_json::Map::new();
    let mut undetm = serde_json::Map::new();
    for (c, n) in LAT_CADENCES.iter().enumerate() {
        caught[c].sort_unstable();
        let k = n.to_string();
        p95m.insert(k.clone(), serde_json::json!(p95(&caught[c])));
        maxm.insert(k.clone(), serde_json::json!(caught[c].last()));
        caughtm.insert(k.clone(), serde_json::json!(caught[c].len() as u64));
        undetm.insert(k, serde_json::json!(trials - caught[c].len() as u64));
    }
    let res = serde_json::json!({
        "experiment": "latencycheck",
        "params": { "seed": seed, "trials": trials, "events_per_trial": LAT_EVENTS, "cadences": LAT_CADENCES,
                    "levels_per_side_initial": 500, "price_window_ticks": STALE_WINDOW, "defect_depth": "uniform over ranks 1-50 of a random side",
                    "injection_event": "uniform over 100-999",
                    "note": "updates_to_catch is the event index of the first comparison that finds divergence minus the injection index; percentiles are nearest-rank over caught trials only; undetected trials are counted separately" },
        "detection": {
            "trials": trials,
            "updates_to_catch_p95_by_cadence": p95m,
            "updates_to_catch_max_by_cadence": maxm,
            "caught_by_cadence": caughtm,
            "undetected_at_end_by_cadence": undetm
        },
        "provenance": {
            "started_at": started_at, "seeds": [seed],
            "command": format!("cargo run --locked -q --release -- latencycheck --seed {seed} --trials {trials} --out {}", out.display()),
            "hardware": format!("cpu-only, {} {}", std::env::consts::OS, std::env::consts::ARCH),
            "wall_s": t0.elapsed().as_secs_f64().round(),
            "data_sources": [
                { "url": "in-process seeded synthetic stream (no external data)", "license": "MIT (own code)" }
            ]
        }
    });
    if let Some(dir) = out.parent() { std::fs::create_dir_all(dir)?; }
    std::fs::write(out, serde_json::to_string_pretty(&res)? + "\n")?;
    println!("{}", serde_json::to_string_pretty(&res["detection"])?);
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Cmd::Record { venue, symbol, out_dir, minutes, top_every, top_levels } => match venue.as_str() {
            "binance" | "binance-us" => record_binance(&venue, symbol, out_dir, minutes, top_every, top_levels).await,
            "kraken" => record_kraken(symbol, out_dir, minutes, top_every, top_levels).await,
            "coinbase" => record_coinbase(symbol, out_dir, minutes, top_every, top_levels).await,
            other => anyhow::bail!("unknown venue {other}"),
        },
        Cmd::Verify { capture } => verify(&capture),
        Cmd::Report { dir, readme } => report(&dir, &readme),
        Cmd::Stalecheck { seed, defects, out } => stalecheck(seed, defects, &out),
        Cmd::Depthcheck { seed, trials, out } => depthcheck(seed, trials, &out),
        Cmd::Latencycheck { seed, trials, out } => latencycheck(seed, trials, &out),
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
    fn rfc3339_parses_to_unix_ms() {
        assert_eq!(rfc3339_to_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(rfc3339_to_ms("2026-10-19T15:04:05.123456Z"), Some(1792422245123));
        assert_eq!(rfc3339_to_ms("2024-02-29T23:59:59.5Z"), Some(1709251199500));
        assert_eq!(rfc3339_to_ms("not a time"), None);
    }

    #[test]
    fn kraken_checksum_matches_a_hand_computed_case() {
        // Kraken docs example shape: price 45283.5 qty 0.10000000 -> "452835" + "10000000".
        let mut b = Book::default();
        b.load(&[("45283.4".into(), "0.20000000".into())], &[("45283.5".into(), "0.10000000".into())]).unwrap();
        let expect = crc32fast::hash(b"4528351000000045283420000000");
        assert_eq!(kraken_checksum(&b, 1, 8), expect);
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

    #[test]
    fn stalecheck_is_deterministic_and_chain_blind() {
        let mut a = SplitMix(7);
        let mut b = SplitMix(7);
        for _ in 0..5 {
            let (x, y) = (stale_trial(&mut a), stale_trial(&mut b));
            assert!(!x.chain);
            assert_eq!((x.snapshot, x.crc), (y.snapshot, y.crc));
        }
        assert!(chain_ok(10, 11) && !chain_ok(10, 12));
    }

    #[test]
    fn depthcheck_is_deterministic_and_pointer_resolves() {
        let mut a = SplitMix(7);
        let mut b = SplitMix(7);
        for _ in 0..5 {
            let (x, y) = (depth_trial(&mut a), depth_trial(&mut b));
            assert!(!x.chain);
            assert_eq!((x.rank, x.crc10, x.crc25, x.snapshot), (y.rank, y.crc10, y.crc25, y.snapshot));
            assert!((1..=DEPTH_MAX_RANK).contains(&x.rank));
        }
        let out = std::env::temp_dir().join(format!("bookrecord-depth-{}.json", std::process::id()));
        depthcheck(7, 3, &out).unwrap();
        let v: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(&out).unwrap()).unwrap();
        assert!(v.pointer("/detection/catch_count_by_depth/crc32_top10").is_some());
        assert!(v.pointer("/provenance/started_at").is_some());
    }

    #[test]
    fn latencycheck_is_deterministic_and_pointer_resolves() {
        let mut a = SplitMix(7);
        let mut b = SplitMix(7);
        for _ in 0..3 {
            assert_eq!(latency_trial(&mut a), latency_trial(&mut b));
        }
        assert_eq!(p95(&[]), None);
        assert_eq!(p95(&[5]), Some(5));
        assert_eq!(p95(&(1..=100).collect::<Vec<u64>>()), Some(95));
        let out = std::env::temp_dir().join(format!("bookrecord-lat-{}.json", std::process::id()));
        latencycheck(7, 3, &out).unwrap();
        let v: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(&out).unwrap()).unwrap();
        assert!(v.pointer("/detection/updates_to_catch_p95_by_cadence/500").is_some());
        assert!(v.pointer("/provenance/started_at").is_some());
    }

    #[test]
    fn checksum_depth_controls_which_ranks_are_covered() {
        let mut t = Book::default();
        let mut l = Book::default();
        let bids: Vec<(String, String)> = (0..30).map(|i| ((99_999 - i).to_string(), "1.00000000".into())).collect();
        let asks: Vec<(String, String)> = (0..30).map(|i| ((100_001 + i).to_string(), "1.00000000".into())).collect();
        t.load(&bids, &asks).unwrap();
        l.load(&bids, &asks).unwrap();
        t.apply(&[((99_999 - 14).to_string(), "0".into())], &[]).unwrap(); // rank 15 bid
        assert_eq!(kraken_checksum_depth(&t, 2, 8, 10), kraken_checksum_depth(&l, 2, 8, 10));
        assert_ne!(kraken_checksum_depth(&t, 2, 8, 25), kraken_checksum_depth(&l, 2, 8, 25));
    }

    #[test]
    fn dropped_delete_changes_crc_of_top_levels() {
        let mut t = Book::default();
        let mut l = Book::default();
        let bids: Vec<(String, String)> = (0..12).map(|i| ((99_999 - i).to_string(), "1.00000000".into())).collect();
        let asks: Vec<(String, String)> = (0..12).map(|i| ((100_001 + i).to_string(), "1.00000000".into())).collect();
        t.load(&bids, &asks).unwrap();
        l.load(&bids, &asks).unwrap();
        t.apply(&[("99999".into(), "0".into())], &[]).unwrap();
        assert_ne!(kraken_checksum(&t, 2, 8), kraken_checksum(&l, 2, 8));
    }
}
