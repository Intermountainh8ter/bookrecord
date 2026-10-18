# bookrecord

**Record L2 order books from exchange websocket streams, and prove the recording is whole.** Snapshot plus diff stream, the exchange's own continuity rule enforced on every event, a logged resync on any gap, zstd-compressed JSON Lines on disk with a manifest, and the time from frame receipt to book applied measured on every event. `verify` re-checks a capture independently of the live run.

![language: Rust](https://img.shields.io/badge/language-Rust-blue) ![license: MIT](https://img.shields.io/badge/license-MIT-blue) ![status: Binance adapter live](https://img.shields.io/badge/venues-Binance%20spot-informational)

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
| Kraken | CRC32 checksum of the top ten levels on every update | planned, not stubbed |
| Coinbase Advanced Trade | sequence numbers on level2 channel | planned, not stubbed |

No keys are needed for any of these public streams.

## Results

<!-- RESULTS:START -->
| venue | symbol | minutes | events | gaps | resyncs | apply p50 / p99 (µs) | verify consistent | capture sha256 |
|---|---|---|---|---|---|---|---|---|
| binance-us | BTCUSDT | 2.0 | 290 | 0 | 0 | 118 / 564 | true | `a13c9339d511e895` |

Rendered by `bookrecord report` from `results/*.manifest.json` and `results/*.verify.json`; capture files are not committed, their hashes are.
<!-- RESULTS:END -->

## License

MIT.
