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

## Previous-update-ID chain (synthetic, seed 7)

`bookrecord puchain` builds a seeded synthetic derivatives-style feed (200 trials, 300 events, overlapping U/u ranges, each event carrying `pu` equal to the previous event's `u`). One event is skipped per trial, chosen so the next event still satisfies the spot range rule. Result in `results/62-puchain.json`: the spot range rule missed 200 of 200 breaks, the `pu` rule missed 0 of 200. On 200 clean controls both rules raised 0 alarms, while a recorder that restarts on every removal of an unknown level restarted 6065 times, in 200 of 200 trials. The pre-registered expectation (range rule misses at least 150 of 200, `pu` rule misses 0 with 0 false restarts) held. The 200 of 200 is a property of the harness, since skip positions are chosen so the range rule cannot see them; the result shows what the `pu` rule adds, not how often real feeds break. The stream is synthetic; this says nothing about real feeds.

Reproduce: `cargo run --locked -q --release -- puchain --seed 7 --trials 200 --out results/62-puchain.json`

## Delivery cadence and stall timeouts (synthetic, seed 7)

`bookrecord cadencecheck` calibrates a silence-timeout stall detector at three times the median inter-message gap of a per-message feed (gaps uniform 2 to 20 ms, 1000 messages, 200 trials), then runs it on the same changes delivered per message and delivered in 50 ms batches, with a 500 ms silence injected in a separate copy of each feed. Result in `results/62-cadencecheck.json`: the detector raised a false-stale alarm in 200 of 200 clean batched trials (43886 alarms in total) and in 0 of 200 clean per-message trials, and missed 0 injected stalls in both arms. The pre-registered expectation (at least 100 of 200 batched, at most 10 of 200 per-message, all stalls detected) held. The result is close to a property of the harness: a batch interval above three median gaps puts every gap over the timeout, and the per-message gaps are bounded by construction. It shows the mechanism, a timeout calibrated on one cadence misreads another, not how often real feeds batch. The stream is synthetic; this says nothing about real feeds.

Reproduce: `cargo run --locked -q --release -- cadencecheck --seed 7 --trials 200 --out results/62-cadencecheck.json`

## Strict versus reorder-tolerant gap rule (synthetic, seed 7)

`bookrecord seqproduct` feeds 300 drop-free streams of 500 numbered messages with adjacent-swap reordering at probability 0.1 per position, and 300 streams with one drop and the same reordering, to a strict rule (a gap whenever the sequence number jumps by more than one) and a reorder-tolerant rule with a hold window of 4. Result in `results/63-seqproduct.json`: the strict rule raised a false gap in 300 of 300 drop-free trials and the tolerant rule in 0 of 300. On the one-drop streams the strict rule detected 300 of 300 drops and the tolerant rule 299 of 300; the tolerant rule missed 0 of the 299 drops outside the last window. The pre-registered expectation (strict at least 280 of 300, tolerant 0) held. The strict count is close to a property of the harness, since any adjacent swap produces a jump of two; the cost of tolerance is the one drop inside the last window of the stream, which stays undetected until more messages arrive. The stream is synthetic; this says nothing about how often real feeds reorder messages.

Reproduce: `cargo run --locked -q --release -- seqproduct --seed 7 --trials 300 --out results/63-seqproduct.json`

## Per-level versus per-message checksum (synthetic, seed 7)

`bookrecord msgchecksum` replays 200 clean seeded streams of 300 multi-level messages (2 to 6 quantity updates each, all inside the top 10 of a dense book), where the checksum on the wire covers the book after the whole message. A verifier that compares the top-10 CRC32 after every level update raised 179495 false mismatches in total; a verifier that compares once after the last update of each message raised 0. On 200 further streams with one corrupted update (quantity plus 1 on a message's last update), the per-message verifier caught 200 of 200 in the corrupted message. Result in `results/65-msgchecksum.json`. The pre-registered expectation (at least 150 per-level false mismatches, 0 per-message) held. The count is close to a property of the harness: any intermediate state differs from the final one. The stream is synthetic; this says nothing about real feeds.

Reproduce: `cargo run --locked -q --release -- msgchecksum --seed 7 --trials 200 --out results/65-msgchecksum.json`

## Planned reconnect (synthetic, seed 7)

`bookrecord plannedreconnect` runs 200 trials of a 400-event update-ID stream with a planned handover (after an ID uniform on 100 to 300). A cold-restart recorder drops the connection and resnapshots a delay of 1 to 5 IDs later; an overlap recorder opens a second connection 1 to 5 IDs before the first closes, with and without deduplication by update ID. Result in `results/67-plannedreconnect.json`: the cold-restart recorder missed 601 updates in total, with missed updates in 200 of 200 trials, and spent 601 events with a stale book; the deduplicating overlap recorder missed 0 and applied 0 twice; the overlap recorder without deduplication missed 0 and applied 568 updates twice. The pre-registered expectation (cold-restart total at least 200; overlap misses and duplicates 0) held. The misses follow from the assumed resnapshot delay and the overlap result from the dedup rule, so this measures the policy mechanism, not any venue's behaviour or notice lead time. The stream is synthetic; this says nothing about real feeds.

Reproduce: `cargo run --locked -q --release -- plannedreconnect --seed 7 --trials 200 --out results/67-plannedreconnect.json`

## Checksum resubscribe vs heartbeat-only (synthetic, seed 7)

`bookrecord checksumresub` injects one silent single-level corruption per trial (200 trials, 300 messages, dense 10-level book, heartbeats never stop) and compares a recorder that checks the top-10 CRC32 after every message and resubscribes on mismatch with one that reconnects only when heartbeats stop. Result in `results/68-checksumresub.json`: the heartbeat-only recorder detected 0 of 200 corruptions and counted 3528 wrong updates, with 13 trials still wrong at stream end; the checksum recorder detected 200 of 200, counted 200 wrong updates (one per trial, the corrupted message itself) and ended with 0 wrong trials. The pre-registered expectation (heartbeat-only wrong updates at least 1) held. The result follows from the construction: the heartbeat-only policy never replaces the book, and the dense book places every level inside the checksummed window. The stream is synthetic; this says nothing about real feeds or venue behaviour.

Reproduce: `cargo run --locked -q --release -- checksumresub --seed 7 --trials 200 --out results/68-checksumresub.json`

## Cadence drift (synthetic, seed 7)

`bookrecord cadencedrift` steps a synthetic stream's update period through three phases (50000, 25000 and 20000 microseconds, 300 messages each, jitter uniform within 20 percent of the period) and injects one stall per phase, a silence of four periods. It compares a stall detector with a fixed threshold of 150000 microseconds against one whose threshold is three times the median of the last 32 intervals (300 trials). Result in `results/70-cadencedrift.json`: of 900 injected stalls the fixed detector missed 600 (miss rate 0.6666666666666666) and the adaptive detector missed 0 (rate 0.0); false alarms were 0 for both. The pre-registered expectation (fixed miss rate at least 0.30, adaptive below 0.05) held. The result follows from the harness: a four-period stall is shorter than the fixed threshold in the two faster phases by construction. The stream is synthetic; this says nothing about real feeds.

Reproduce: `cargo run --locked -q --release -- cadencedrift --seed 7 --trials 300 --out results/70-cadencedrift.json`

## Schema version steps (synthetic, seed 7)

`bookrecord schemaretire` runs 200 seeded trials of 400 frames each, with two schema version steps per trial at random frames. Each version lays the same three fields out in a different order. A recorder pinned to the first version decodes by field position and never errors; a strict-version recorder rejects any frame of another version; a version-aware recorder decodes by the version in each frame. Result in `results/73-schemaretire.json`: the pinned recorder silently mis-decoded 60403 frames, the strict recorder 0 (it rejected 60403 frames loudly, so it stops recording rather than recording wrong values), and the version-aware recorder 0. The pre-registered expectation (pinned at least 400, strict and version-aware 0) held. The result is close to a property of the harness: any positional decode under a changed layout is wrong. The stream is synthetic; this says nothing about how real venues roll out schema changes.

Reproduce: `cargo run --locked -q --release -- schemaretire --seed 7 --trials 200 --out results/73-schemaretire.json`

## Apply budget versus venue cadence (synthetic, seed 7)

`bookrecord cadencestep` replays the same seeded synthetic frames (200 trials, 300 frames each, 20 to 80 messages per frame, per-message apply time uniform 100 to 600 microseconds) against apply budgets equal to the 50, 25 and 20 ms venue periods. Result in `results/79-cadencestep.json`: of 60000 frames, 0 exceeded the 50 ms budget, 8660 exceeded the 25 ms budget and 22752 exceeded the 20 ms budget. The pre-registered expectation (at least 10000 over budget at 20 ms, above the 25 ms count) held. The counts follow from the chosen frame-size and per-message time distributions, so this shows how a budget set at a slower cadence undercounts overruns at a faster one, not how real feeds behave. The stream is synthetic; this says nothing about real feeds.

Reproduce: `cargo run --locked -q --release -- cadencestep --seed 7 --trials 200 --out results/79-cadencestep.json`

## Receive-queue bursts (synthetic, seed 7)

`bookrecord burstgap` feeds a bounded receive queue (capacity 32 frames, drop the newest when full, 200 µs per frame) at message-rate bands of 500, 1500, 3000 and 4900 messages per second, 200 one-second trials per band, with clustered arrivals (bursts of 20 frames, 20 µs apart, placed in the middle half of non-overlapping slots) and evenly spaced arrivals at the same mean rate. Result in `results/87-burstgap.json`: the gap count was 0 in every band for both arrival patterns, with 0 dropped frames in every band. The pre-registered falsifier (a top-band gap count not above the lowest band) fired, so the hypothesis is refuted for this harness. The result is close to a property of the harness: a burst of 20 frames fits a queue of 32 and the mean service rate exceeds the top band, so nothing overflows. The final burst placement and the top band rate were chosen after an earlier run of the same command, whose clustered placement let adjacent bursts overlap and drop frames in the lowest band; that earlier output was overwritten. The stream is synthetic; this says nothing about how often real feeds drop frames.

Reproduce: `cargo run --locked -q --release -- burstgap --seed 7 --trials 200 --out results/87-burstgap.json`
