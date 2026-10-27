# bookrecord

**Record L2 order books from exchange websocket streams, and prove the recording is whole.** Snapshot plus diff stream, the exchange's own continuity rule enforced on every event, a logged resync on any gap, zstd-compressed JSON Lines on disk with a manifest, and the time from frame receipt to book applied measured on every event. `verify` re-checks a capture independently of the live run.

![language: Rust](https://img.shields.io/badge/language-Rust-blue) ![license: MIT](https://img.shields.io/badge/license-MIT-blue) ![status: Binance, Kraken, Coinbase adapters live](https://img.shields.io/badge/venues-Binance%20spot-informational)

## Why

Every backtest and every execution test downstream of this tool is only as good as the book it replays. Most recorders write what arrives and hope. This one applies the venue's sequencing rule (Binance: the first kept event must bracket the snapshot's update ID and every later event must start at the previous event's last ID plus one), writes a `gap` record and resyncs from a fresh snapshot when the rule breaks, refuses to continue if the book ever crosses, and keeps per-event processing latency so the recorder's own overhead is a number, not a feeling.

## Usage

```
bookrecord record --venue binance-us --symbol BTCUSDT --out-dir captures --minutes 60
bookrecord verify captures/binance-us-btcusdt-<unix>.jsonl.zst
```

`record` writes `<stem>.jsonl.zst` (meta, snapshot, every raw event with receive time and apply latency, a top-20 book every 500 events, gaps, end summary) and `<stem>.manifest.json` (counts, gaps, resyncs, SHA-256, latency p50 / p90 / p99 / max, the continuity rule in words).

`verify` re-reads the capture and reports: chain breaks among applied events (must be zero), whether every logged gap is well formed, whether the end record matches the file, and the latency percentiles. Exit 1 if any check fails.

## Venues

| Venue | Rule enforced | Status |
| --- | --- | --- |
| Binance.US spot (`binance-us`) | update-ID chain (`U`, `u`) against a REST snapshot | implemented, captured |
| Binance.com spot (`binance`) | same | implemented; answers HTTP 451 from the United States |
| Kraken (`kraken`, symbol like `BTC/USD`) | CRC32 checksum of the top ten levels on every update, resubscribe on mismatch | implemented, captured |
| Coinbase Advanced Trade (`coinbase`, symbol like `BTC-USD`) | per-connection `sequence_num` must advance by one on every message; a skipped number is a gap and triggers resubscribe for a fresh snapshot | implemented, captured |

No keys are needed for any of these public streams.

## Results

<!-- RESULTS:START -->
| venue | symbol | minutes | events | gaps | resyncs | apply p50 / p99 (µs) | verify consistent | capture sha256 |
|---|---|---|---|---|---|---|---|---|
| binance-us | BTCUSDT | 2.0 | 290 | 0 | 0 | 118 / 564 | true | `a13c9339d511e895` |
| coinbase | BTC-USD | 2.0 | 2312 | 0 | 0 | 518 / 4012 | true | `aa9f404f48b8e866` |
| kraken | BTC/USD | 2.0 | 1417 | 0 | 0 | 394 / 700 | true | `eba8119e3ddb710c` |

Rendered by `bookrecord report` from `results/*.manifest.json` and `results/*.verify.json`; capture files are not committed, their hashes are.
<!-- RESULTS:END -->

## Stale-level detection (synthetic, seed 7)

`bookrecord stalecheck` injects 200 dropped-delete defects, one per independent trial, into a seeded synthetic diff stream. The defect leaves the update-ID chain intact. Result in `results/35-stale-level-detection.json`: the update-ID chain check detected 0 of 200 (rate 0.0, by construction), the CRC32 over the top ten levels detected 200 of 200 (rate 1.0), and a full snapshot comparison every 500 events detected 170 of 200 (rate 0.85). The pre-registered expectation of at least 0.90 for the snapshot check was not met. The stream is synthetic; this says nothing about how often real feeds produce such defects.

Reproduce: `cargo run --locked -q --release -- stalecheck --seed 7 --defects 200 --out results/35-stale-level-detection.json`

## Depth-resolved detection (synthetic, seed 7)

`bookrecord depthcheck` injects one chain-clean dropped-delete defect per trial (500 trials) at a book depth rank drawn uniformly from 1 to 50, into the same seeded synthetic stream, with ordinary book churn continuing afterwards. Result in `results/38-depth-resolved-detection.json`: the update-ID chain caught 0 of 500, a CRC32 over the top 10 levels caught 105 of 500 (rate 0.21), a CRC32 over the top 25 caught 263 of 500 (rate 0.526), and a full snapshot comparison every 500 events caught 437 of 500 (rate 0.874). Of 95 trials injected at ranks 1 to 10, the top-10 checksum caught 95; it also caught 10 trials injected at ranks above 10. The top-25 checksum caught 251 of 251 trials at ranks 1 to 25 and 12 trials above rank 25. The pre-registered falsifier (a top-10 catch at a rank above 10) fired, so the strict hypothesis is refuted: later churn moves a stale level into the checksummed window. The stream is synthetic; this says nothing about how often real feeds produce such defects.

Reproduce: `cargo run --locked -q --release -- depthcheck --seed 7 --trials 500 --out results/38-depth-resolved-detection.json`

## Snapshot-comparison latency (synthetic, seed 7)

`bookrecord latencycheck` injects one chain-clean dropped-delete defect per trial (500 trials, rank uniform 1 to 50, 4000-event stream) and records the updates from injection to the first full snapshot comparison that finds the divergence, for cadences of 100, 250, 500, 1000 and 2000 events. Result in `results/39-snapshot-detection-latency.json`: at cadence 500 the 95th percentile was 447 updates (maximum 498), with 429 of 500 caught and 71 undetected at stream end. Undetected at stream end, by cadence: 16 at 100, 31 at 250, 130 at 1000, 345 at 2000. The pre-registered expectation (p95 at most 500 at cadence 500, undetected count above zero at each cadence) held. The upper bound is close to a property of the harness, since a comparison fires every cadence; the informative number is the undetected count, which shows later churn overwrote the stale level before a comparison in those trials. The stream is synthetic; this says nothing about how often real feeds produce such defects.

Reproduce: `cargo run --locked -q --release -- latencycheck --seed 7 --trials 500 --out results/39-snapshot-detection-latency.json`

## Defect classes and clean controls (synthetic, seed 7)

`bookrecord classcheck` injects one chain-clean defect per trial (500 trials per class, rank uniform 1 to 50, 4000-event stream) of three classes (dropped delete, dropped insert, dropped quantity change), and runs 500 defect-free control streams through the same three detectors. Result in `results/40-defect-class-controls.json`: the top-10 CRC32 caught 105 of 500 dropped deletes, 94 of 500 dropped inserts and 111 of 500 dropped quantity changes; the snapshot comparison every 500 events caught 437, 409 and 425 of 500; the update-ID chain caught 0 in every class. Across the 500 clean controls the three detectors raised 0 alarms in total. The pre-registered expectation (0 false positives, at least 75 of 500 top-10 CRC32 catches for dropped insert and dropped quantity change) held. The controls share the harness with the injected trials, so a zero shows the detectors do not misfire on a faithful copy, not that real feeds are free of false alarms. The stream is synthetic; this says nothing about how often real feeds produce such defects.

Reproduce: `cargo run --locked -q --release -- classcheck --seed 7 --trials 500 --out results/40-defect-class-controls.json`

## Retention corridor (synthetic, seed 7)

`bookrecord retentioncheck` runs a naive local book and local books pruned to corridor depths 25, 50 and 100 levels per side over a seeded synthetic stream (200 trials, 1500 events each) with a drifting mid price. The feed sends upserts for levels entering the top-100 window and deletes only for removed levels, never for levels that leave the window. Result in `results/42-retention-corridor.json`: at the final audit the true book held 40000 levels across all trials; the naive book held 61269 local levels of which 40000 matched (matching-to-local ratio 0.6528587050547585); the corridor books held 10000, 20000 and 40000 levels at depths 25, 50 and 100, all matching (ratio 1.0). The pre-registered expectation held: the 50-level corridor matched 100.0 levels per trial on average and its ratio exceeded the naive ratio, and the naive book held more local levels than the true book. The corridor at 100 equals the venue window by construction, and a shallower corridor trades coverage for exactness: the 25-level corridor matched 10000 of the 40000 true levels. The stream is synthetic; this says nothing about real feeds.

Reproduce: `cargo run --locked -q --release -- retentioncheck --seed 7 --trials 200 --out results/42-retention-corridor.json`

## License

MIT.
