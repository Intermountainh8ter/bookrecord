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

## Reorder and duplicate delivery (synthetic, seed 7)

`bookrecord ordercheck` replays a seeded synthetic absolute-quantity stream (200 trials, 1000 events, 40 levels) with delivery shuffled within consecutive blocks of the window size and 10 percent of messages delivered twice. It compares a recorder that drops any message whose sequence number is below the next expected one with a recorder that applies by level key under a per-level sequence guard, by final book hash against the in-order reference. Result in `results/44-order-invariance.json`: the drop-lower recorder matched the reference in 200 of 200 trials at window 1, 0 of 200 at window 4 and 0 of 200 at window 8; the keyed-guard recorder matched in 200 of 200 trials at all three windows. The pre-registered expectation (at most 100 matches for drop-lower at window 8, 200 for keyed-guard) held. At window 1 the only perturbation is duplicates, which the drop-lower rule discards. The result is close to a property of the harness: any dropped last-writer update changes the final hash. The stream is synthetic; this says nothing about how often real feeds reorder messages.

Reproduce: `cargo run --locked -q --release -- ordercheck --seed 7 --trials 200 --out results/44-order-invariance.json`

## License

MIT.

## Snapshot reordered against updates (synthetic, seed 7)

`bookrecord snapshotcheck` replays a seeded synthetic absolute-quantity stream (200 trials, 1000 events, 40 levels) with a full snapshot after every 25th update. Snapshots and updates are shuffled together within consecutive blocks of the window size and 10 percent of messages are delivered twice. It compares a recorder that replaces its book on every snapshot with a recorder that tags each level with its sequence number and ignores snapshot levels older than the level's last write, by final book hash against the in-order reference. Result in `results/45-snapshot-reorder.json`: the replace-on-snapshot recorder matched the reference in 200 of 200 trials at window 1, 195 of 200 at window 4 and 177 of 200 at window 8; the guarded-merge recorder matched in 200 of 200 trials at all three windows. The pre-registered expectation (at most 100 matches for replace-on-snapshot at window 8) was not met, so the hypothesis is refuted: later updates rewrite most levels a stale snapshot damaged, so only damage near the end of the stream survives to the final hash. The final hash understates transient divergence mid-stream, which this run does not measure. The stream is synthetic; this says nothing about how often real feeds reorder snapshots.

Reproduce: `cargo run --locked -q --release -- snapshotcheck --seed 7 --trials 200 --out results/45-snapshot-reorder.json`

## Start-up ordering (synthetic, seed 7)

`bookrecord startupcheck` compares three start-up procedures over a seeded synthetic absolute-quantity stream (300 trials, 100 events, 40 levels, snapshot delay 1 to 50 events) and counts final books that pass the update-ID chain check over applied events but differ from the ground-truth book. Result in `results/46-startup-ordering.json`: a recorder that joins the stream after taking the snapshot ended gap-free but wrong in 249 of 300 trials (51 right); the subscribe-then-snapshot recorder that replays buffered events from the start without discarding ended gap-free but wrong in 0 of 300 trials; the buffer-snapshot-discard recorder ended wrong in 0 of 300. The pre-registered expectation (at least 100 wrong for the naive subscribe-then-snapshot start) was not met and its falsifier fired: with absolute quantities, replaying every buffered event in order after the snapshot converges to the same final book, so the stale replay only matters for the transient book. The wrong gap-free books came from the join-after-snapshot ordering, where events between snapshot and subscription are lost but the applied chain stays unbroken. The stream is synthetic; this says nothing about real feeds.

Reproduce: `cargo run --locked -q --release -- startupcheck --seed 7 --trials 300 --out results/46-startup-ordering.json`

## Dropped messages and an optional sequence field (synthetic, seed 7)

`bookrecord seqcheck` drops one message per trial (200 trials, 250 absolute-quantity messages, 40 levels) with the sequence number absent on 20 percent of messages, and counts trials whose final book is wrong while a detector stays silent. The sequence-only detector alarms only when two consecutively received messages both carry a number and it does not advance by one; the heartbeat detector compares a sent count with the received count every 40 messages; the full-hash detector compares book hashes every 100 messages. Result in `results/47-seqcheck.json`: 21 of 200 trials ended with a wrong final book. Of those, the sequence-only detector stayed silent in 7, the heartbeat detector in 4 and the full-hash detector in 13. All three raised 0 alarms across 200 clean controls. The pre-registered expectation (at least 30 silent for sequence-only) was not met, so the hypothesis is refuted: most dropped messages are overwritten by later writes to the same level, which leaves few wrong books to miss. The stream and detectors are synthetic and the detector rules are this tool's own simplification of the venue's optional field; this says nothing about real feeds.

Reproduce: `cargo run --locked -q --release -- seqcheck --seed 7 --trials 200 --out results/47-seqcheck.json`

## Heartbeat detection delay (synthetic, seed 7)

`bookrecord heartbeatdelay` drops one message per trial from a 2000-message synthetic stream (500 trials per interval) that carries a sent-count heartbeat after every H-th message, H in 10, 50 and 200, each heartbeat lost with probability 10 percent. Result in `results/48-heartbeat-delay.json`: trials whose detection delay exceeded one interval or never occurred numbered 46 at H=10, 64 at H=50 and 50 at H=200. Of those, never detected: 1, 3 and 5. The largest detected delay was 28, 143 and 585 messages. No trial exceeded one interval without a lost heartbeat (0 at every H). The pre-registered expectation (30 to 75 at H=50) held. A drop after the last heartbeat is never detected, which adds to the counts at larger H. The stream is synthetic; this says nothing about real feeds.

Reproduce: `cargo run --locked -q --release -- heartbeatdelay --seed 7 --trials 500 --out results/48-heartbeat-delay.json`

## Gap policies (synthetic, seed 7)

`bookrecord gappolicy` drops one message per trial (400 trials, 2000-message stream, 40 levels, position uniform) and replays it through three modelled policies: ignore, strict ID-gap (a received id above last applied plus one marks the book stale), and reconnect on a heartbeat sent-count mismatch (heartbeat every 50 messages, lost with probability 0.1). Result in `results/49-gap-policy.json`: the final book was wrong in 9 of 400 trials. Silent-wrong counts (final book wrong, policy never alarmed): ignore 9, strict ID-gap 0, heartbeat reconnect 1. False-stale counts (alarmed, final book equal to reference): ignore 0, strict ID-gap 391, heartbeat reconnect 391. The pre-registered expectation for the strict metric (at most 5) held, but the falsifier fired on the ignore policy: it was silent-wrong in 9 trials, not at least 300, because later updates overwrite the dropped level in most trials. The strict and heartbeat policies therefore trade almost every alarm for a book that was already right at the end. The stream is synthetic; this says nothing about real feeds.

Reproduce: `cargo run --locked -q --release -- gappolicy --seed 7 --trials 400 --out results/49-gap-policy.json`

## Heartbeat counter versus per-message sequence (synthetic, seed 7)

`bookrecord countercheck` drops one data message per trial (200 trials, 250 data messages, a heartbeat after every 40, heartbeats lossless) and runs two detectors: one that only checks that the heartbeat counter advances by one per heartbeat, and one that checks every message's sequence number. A clean control stream per trial checks for false alarms. Result in `results/57-countercheck.json`: the heartbeat-counter-only check missed 200 of 200 drops, the per-message sequence check missed 0 of 200, and neither raised an alarm on the 200 clean controls. The pre-registered expectation (200 missed by the counter-only check) held. This is close to a property of the harness: a counter that carries no message count cannot see a data drop. The dropped position excludes the last data message, since a sequence check needs a successor to expose a jump. The stream is synthetic; this says nothing about real feeds.

Reproduce: `cargo run --locked -q --release -- countercheck --seed 7 --trials 200 --out results/57-countercheck.json`

## Truncation and the top-10 checksum (synthetic, seed 7)

`bookrecord truncatecheck` streams a depth-25 feed (200 trials, 1500 messages each, symmetric random-walk mid price) that sends upserts for levels entering the window and deletes only for removed levels, never for levels leaving it. It scores the top-10 checksum against the true book after every message for a recorder that never truncates and one that truncates to 25 levels per side after each update. Result in `results/60-truncatecheck.json`: the non-truncating recorder raised at least one checksum mismatch in 190 of 200 trials (23368 mismatching messages in total, mean 487.9894736842105 messages to the first mismatch); the truncating recorder raised 0 mismatches. The pre-registered expectation (at least 1 trial with a mismatch without truncation, 0 with truncation) held. Unlike `retentioncheck`, which audits the final book, this scores the checksum over time. The zero for the truncating arm is close to a property of the harness, since its local book equals the window by construction. The stream is synthetic; this says nothing about real feeds.

Reproduce: `cargo run --locked -q --release -- truncatecheck --seed 7 --trials 200 --out results/60-truncatecheck.json`
