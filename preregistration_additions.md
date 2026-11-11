# Pre-registration additions

## 2026-10-27T07:33:54Z stale-level-detection proposal:ceb7c6a3448191185c08feba589912e74d1f41b712e08053ebe4d9ea344af9b7

Hypothesis: on a seeded synthetic diff stream with 200 injected dropped-delete defects that preserve the U/u chain, the update-ID chain check detects 0 of them (rate 0.0), while a snapshot comparison every 500 events and a CRC32 over the top ten levels each detect at least 0.90 of them.

Metric: /detection/rate_by_check/update_id_chain. Expected: equals 0.0.

Falsifier: the update-ID chain check detects any injected chain-preserving defect (rate above 0.0), or the snapshot or CRC32 rate falls below 0.90 under seed 7.

## 2026-10-27T07:47:57Z depth-resolved-detection proposal:923fd8e1dbbb3bf7c5de7c393dedccb4ab5ad4cebe7478a41ccf90ddf1c24d35

Hypothesis: a CRC32 over the top 10 levels catches every injected chain-clean dropped-delete defect at depth rank 1 to 10 and none at rank 11 to 50, while a top-25 checksum catches all at rank 1 to 25 and none at 26 to 50, so the top-10 catch count equals the number of trials injected at ranks 1 to 10.

Metric: /detection/catch_count_by_depth/crc32_top10. Expected: at most 20 percent of trials (ranks uniform over 1-50), expected_range [0.0, 0.25].

Falsifier: a top-10 CRC32 catch at any injected depth rank greater than 10, or a miss at any rank 1 to 10, refutes the hypothesis.

## 2026-10-27T07:54:03Z snapshot-detection-latency proposal:6501381f9b8011638dc97c958bfc0c3aebdd11957e673c6ea8bb37b0d264f132

Hypothesis: on the seeded synthetic stream (seed 7, 500 trials per cadence, cadences 100, 250, 500, 1000, 2000 events), the 95th percentile of updates from injection to detection by a periodic full snapshot comparison at cadence 500 is at most 500, and the count of defects undetected at stream end is above zero at every cadence because later churn can overwrite the stale level before the next comparison.

Metric: /detection/updates_to_catch_p95_by_cadence/500. Expected: at most 500, expected_range [1, 500].

Falsifier: a p95 above 500 at cadence 500 (harness error), or an undetected-at-end count of zero at every cadence.

## 2026-10-27T07:59:28Z defect-class-controls proposal:2535fe10437a90c5ee042e738ca2d1b9c701516f97d880b3519ac22887c88292

Hypothesis: on 500 defect-free seeded control streams (seed 7, 4000 events each) none of the update-ID chain, top-10 CRC32 or every-500-event snapshot comparison raises an alarm (0 false positives in total), while the dropped-insert and dropped-quantity classes are each caught by the top-10 CRC32 in at least 15 percent of 500 trials per class.

Metric: /clean_control/false_positive_count_total. Expected: equals 0, expected_range [0, 0].

Falsifier: any alarm from any detector on a clean control stream (false_positive_count_total above 0), or a per-class top-10 CRC32 catch count below 75 of 500 for dropped-insert or dropped-quantity.

## 2026-10-27T08:11:48Z retention-corridor proposal:6adc8868193cc93780d673bea47cfcc04720cd0df7de36199202360c240b9827

Hypothesis: on a seeded synthetic stream with a drifting mid price where out-of-scope levels receive no delete (seed 7, 200 trials), a book truncated to 50 levels from the best price has a higher matching-to-local level ratio at the final audit than the naive book, and the naive book holds more local levels than the true book.

Metric: /retention/matching_level_count_by_corridor/corridor_50. Expected: at least 1 matching level on average per trial, and the 50-level corridor ratio exceeds the naive ratio; expected_range [1, 1000].

Falsifier: the 50-level corridor ratio is not greater than the naive ratio, or the naive book holds no more local levels than the true book.

## 2026-10-27T08:22:26Z order-invariance proposal:95f57c771763739ad405ac40ae1ee0cd176674fef54ec73192ba5ae2becae14a

Hypothesis: on a seeded synthetic absolute-quantity stream (seed 7, 200 trials of 1000 events, reorder window 8), a recorder that drops messages with a lower-than-expected sequence number ends with a book hash equal to the reference in at most 100 of 200 trials, while a recorder that applies by level key with a per-level sequence guard matches in 200 of 200 trials.

Metric: /summary/drop_lower_window8_match_count. Expected: at most 100, expected_range [0, 100]; keyed-guard arm reaches 200.

Falsifier: the drop-lower recorder matches in more than 100 of 200 trials at window 8, or the keyed-guard recorder matches in fewer than 200.

## 2026-10-27T08:27:43Z snapshot-reorder proposal:2c7299da4a5b9f6c3c0138d1b4fd12128babb40ecc7a96ab36d0dd5203934447

Hypothesis: on a seeded synthetic absolute-quantity stream with periodic full resnapshots (seed 7, 200 trials), delivery shuffled within blocks of window 8 makes a recorder that replaces its book on every snapshot match the in-order reference final book hash in at most 100 of 200 trials, while a recorder that tags each level with its sequence number and ignores snapshot levels older than the level's last write matches in 200 of 200 trials.

Metric: /summary/snapshot_replace_hash_match_count_window8. Expected: at most 100, expected_range [0, 100]; guarded-merge arm reaches 200.

Falsifier: the replace-on-snapshot arm matches in more than 100 of 200 trials at window 8, or the guarded-merge arm matches in fewer than 200 of 200.

## 2026-10-27T08:32:20Z startup-ordering proposal:2dc39b8ae15b3780dd1899b34c3fb875992749954a47823137e5d6fb741773b7

Hypothesis: over 300 seeded trials (seed 7) with snapshot delays of 1 to 50 events, the naive subscribe-then-snapshot start-up with no discard yields at least 100 final books that pass the update-ID gap check yet differ from the ground-truth book, while the buffer-snapshot-discard procedure yields 0.

Metric: /summary/gapfree_but_wrong_count_naive_start. Expected: at least 100 of 300, expected_range [100, 300]; correct-procedure count is 0.

Falsifier: gapfree_but_wrong_count_naive_start below 100 of 300, or the buffer-snapshot-discard procedure producing any wrong gap-free book.

## 2026-10-27T08:37:30Z seqcheck proposal:334f13ccc075c14b9ea27e7c8b6dc3588f376c67f5235acd442e64485fbe57fd

Hypothesis: with sequence_num absent on 20 percent of messages and one message dropped per trial (200 trials, seed 7), the sequence-only detector (alarms only when two consecutively received messages both carry a sequence number and it does not advance by one) leaves at least 30 trials with a wrong final book and no alarm, because drops of field-less messages are invisible to it. Stream parameters fixed before running: 250 messages, 40 levels, heartbeat counter every 40 messages, full-book hash every 100 messages.

Metric: /summary/silent_wrong_book_count_sequence_only. Expected: at least 30 of 200, expected_range [30, 200].

Falsifier: fewer than 30 of 200 trials have a wrong final book with no sequence-only alarm, or the sequence-only detector raises an alarm on any clean control stream.

## 2026-10-27T08:41:44Z heartbeat-delay proposal:973d8e83238c4bed6235a59b6558284f405184d59896533109103df4786807c7

Hypothesis: with heartbeat loss probability 0.1 and 500 trials per interval H (10, 50, 200; seed 7; 2000 messages, one drop per trial), the number of trials at H=50 whose drop stays undetected for more than 50 messages (including never detected) lies between 30 and 75, because a lost next heartbeat doubles the delay. It differs from seqcheck, which counts wrong final books rather than time to detect.

Metric: /summary/undetected_over_one_interval_count_h50. Expected: roughly 50 of 500, expected_range [30, 75].

Falsifier: a count at H=50 below 30 or above 75 of 500 trials, or any trial detected after more than one interval with no heartbeat lost.

## 2026-10-27T08:46:32Z gap-policy proposal:a5c934bf1a9765d9dd3c99c631c3c0d43fd32e1c2777bad38cab55833faf69ed

Hypothesis: over 400 seeded trials (2000-message stream, 40 levels, one drop per trial, drop position uniform), the strict ID-gap policy leaves at most 5 trials silent-wrong (final book wrong while the policy never alarmed), the ignore policy leaves at least 300 silent-wrong, and the strict policy raises at least 20 false-stale trials (alarmed though the final book equals the reference because later updates overwrote the dropped level). Differs from seqcheck (no policy, final-book wrongness only) and heartbeatdelay (detection delay). Heartbeat every 50 messages, lost with probability 0.1, is the third modelled policy.

Metric: /summary/strict_gap_policy_silent_wrong_count. Expected: at most 5 of 400, expected_range [0, 5].

Falsifier: strict_gap_policy_silent_wrong_count above 5, or ignore-policy silent-wrong below 300, or strict false-stale below 20.

## 2026-10-27T09:23:38Z countercheck proposal:a60031ac0c1aa362ccdaabf795f8d0ba6413a5b3a7746d06e31a3549883d6fe8

Hypothesis: in a seeded synthetic stream (200 trials, seed 7, 250 data messages, a heartbeat after every 40 data messages, heartbeats lossless, one data message dropped per trial at a position uniform over all but the last data message), a detector that only checks that heartbeat_counter advances by one per heartbeat misses all 200 data drops, while a per-message sequence_num check with every message numbered misses 0; both raise 0 alarms on 200 clean controls. Unlike seqcheck (sent-count heartbeat) the counter carries no message count.

Metric: /summary/data_drops_missed_by_heartbeat_only_count. Expected: equals 200, expected_range [200, 200].

Falsifier: any injected data-message drop that the heartbeat-counter-only detector alarms on (missed count below 200), or any alarm on a clean control.

## 2026-10-27T09:39:55Z truncatecheck proposal:5b91cd6ceed7288564e44cf4d56b252a4067a176f23bb3a8246393f257737db1

Hypothesis: over 200 seeded trials of 1500 messages with a drifting mid price and a depth-25 subscription (no delete sent for levels leaving the window), a non-truncating recorder raises at least one top-10 checksum mismatch in at least 1 trial, while a recorder that truncates its local book to 25 levels per side after each update raises zero mismatches in all 200 trials. Differs from retentioncheck, which audits only the final book; this scores the top-10 checksum after every message.

Metric: /summary/checksum_mismatch_count_no_truncate (trials with at least one mismatch). Expected: between 1 and 200, expected_range [1, 200]; truncating arm count 0.

Falsifier: the non-truncating arm raises zero mismatches across all 200 trials, or the truncating arm raises any mismatch.

## 2026-11-11T09:50:05Z puchain proposal:94118b901787a79c1c42a0af1d6d16fd900cf358689d8ef58f9510ce826c1816

Hypothesis: on a seeded synthetic derivatives-style feed (200 trials, seed 7, one injected chain break per trial that skips an event while keeping the next event's U <= prev_u+1 <= u overlap), the spot range-overlap rule misses at least 150 of 200 breaks while the pu==previous u rule misses 0, and the pu rule raises 0 false restarts on clean controls holding benign unknown-level removals.

Metric: /summary/pu_break_missed_by_range_rule_count. Expected: at least 150 of 200, expected_range [150, 200].

Falsifier: the range rule misses fewer than 150 of 200 breaks, or the pu rule misses any break or raises any false restart on clean controls.

## 2026-11-11T10:00:33Z cadencecheck proposal:5451839b082bd3f36c19964c2baf71e48f6c0a2c602771394c83906dc541c32b

Hypothesis: a stall detector whose silence timeout is fixed at three times the median inter-message gap of a per-message feed raises at least one false-stale alarm in at least 100 of 200 clean batched-feed trials (50 ms batches), while raising false-stale alarms in at most 10 of 200 clean per-message trials, and both configurations detect all injected true stalls.

Metric: /summary/false_stale_count_batched. Expected: at least 100 of 200, expected_range [100, 200]; per-message false-stale count at most 10.

Falsifier: false_stale_count_batched below 100 of 200 trials, or per-message false-stale count above 10 of 200.

## 2026-11-11T10:05:35Z seqproduct proposal:27b74257e7a31a604318b738c007c3d491f945f1631896bc3aae24dc004f13ee

Hypothesis: on 300 drop-free synthetic streams of 500 messages with adjacent-swap reordering at probability 0.1 per position (seed 7), the strict greater-than-one rule raises a false gap in at least 280 of 300 trials, while a reorder-tolerant rule with a hold window of 4 raises 0 false gaps; on 300 streams with exactly one drop and the same reordering, the tolerant rule detects every drop that is not in the last window of the stream.

Metric: /summary/false_gap_count_strict. Expected: at least 280 of 300, expected_range [280, 300]; false_gap_count_tolerant equals 0.

Falsifier: false_gap_count_strict below 280 of 300, or false_gap_count_tolerant above 0 on drop-free reordered streams.

## 2026-11-11T10:21:14Z msgchecksum proposal:4869ae31075e8a6ca02112ad3557903f0ce800ee85913925a430df1a38888659

Hypothesis: on 200 seeded clean streams of 300 multi-level messages (2 to 6 level updates each), a verifier that checks the top-10 CRC32 after every level update raises at least 150 false mismatches in total, while the per-message verifier raises 0, and on 200 streams with one injected single-level corruption the per-message verifier detects the corruption in the corrupted message in every trial.

Metric: /arms/per_level/false_mismatch_count. Expected: at least 150, expected_range [150, 1000000]; per-message count equals 0.

Falsifier: a per-level false mismatch count below 150, or any false mismatch for the per-message verifier on a clean stream, or a corruption missed by the per-message verifier in its own message.

## 2026-11-11T10:30:49Z plannedreconnect proposal:916558ade7f0849b13f74c9d805f6243f19b3165c0d876b30082ceb633a5c608

Hypothesis: on a seeded synthetic stream with a planned connection handover (200 trials, seed 7), a cold-restart recorder (drop, resnapshot) misses at least one update in every trial, while an overlap recorder (second connection opened before the first closes, deduplicated by update id) misses none and applies none twice.

Metric: /plannedreconnect/cold_restart/missed_updates. Expected: total at least 200 over 200 trials, expected_range [200, 1000000]; overlap missed_updates and duplicate_applications both equal 0.

Falsifier: overlap missed_updates or duplicate_applications above 0, or cold-restart total missed_updates below 200 across 200 trials.

## 2026-11-11T10:35:30Z checksumresub proposal:2fc49204864ef2b1d8317436d0087f179129400c85ba9695dea852c5d0fdcab8

Hypothesis: under a heartbeat-deadline-only policy a silent single-level corruption is never detected within a stream that keeps delivering heartbeats, so undetected wrong updates are at least 1 in every injected trial, while the checksum-and-resubscribe policy leaves wrong updates only until the next message whose checksum covers the corrupted level.

Metric: /policies/heartbeat_only/undetected_wrong_updates. Expected: at least 1, expected_range [1, 1000000000].

Falsifier: heartbeat_only undetected_wrong_updates equal to 0 in the results file, or the checksum policy detecting zero corruptions.

## 2026-11-11T10:45:43Z cadencedrift proposal:8278e4ee25a6e41e7e5a5d12776b8d2e9142fcd64c7197586e80db5f29f43ac0

Hypothesis: on a seeded synthetic stream whose update period steps from 50 ms to 25 ms to 20 ms with jitter, a stall detector with a threshold fixed at three times the 50 ms period misses injected stalls at the faster cadences (miss rate at least 0.30 across trials), while a rolling-median adaptive detector misses under 0.05 of injected stalls.

Metric: /summary/miss_rate_fixed. Expected: at least 0.30, expected_range [0.30, 1.0]; adaptive miss rate below 0.05.

Falsifier: a fixed-threshold miss rate below 0.30, or an adaptive-detector miss rate of 0.05 or more, across the 300 trials at seed 7.

## 2026-11-11T10:59:10Z schemaretire proposal:174a443ffc8fcf1efd5e1886f623fe03e39ee4559b9ebf061e2bd4e0a5c9a792

Hypothesis: on a seeded synthetic stream of 200 trials with two schema version steps at random events, a recorder that pins the first version and decodes by field position mis-decodes at least one frame silently after each step in every trial, while a strict-version recorder and a version-aware recorder record 0 silent mis-decodes at both steps.

Metric: /pinned/silent_misdecodes. Expected: total at least 400 across 200 trials, expected_range [400, 1000000]; strict and version-aware totals equal 0.

Falsifier: a pinned total below 400 across the 200 trials, or any silent mis-decode by the strict or version-aware recorder.

## 2026-11-11T11:29:19Z cadencestep proposal:b2fe64db484d4a6319799ee1ba5a1fe4d08bee1579a393ee2e7290c535191504

Hypothesis: for identical seeded synthetic frames (200 trials, 300 frames each, 20 to 80 messages per frame, per-message apply time uniform 100 to 600 microseconds), the count of frames over budget is strictly larger at the 20 ms budget than at the 25 ms budget, and the 25 ms count is strictly larger than the 50 ms count.

Metric: /budgets/over_budget_frames_20ms. Expected: at least 10000 of 60000 frames and above the 25 ms count, expected_range [10000, 30000].

Falsifier: the 20 ms count is not strictly greater than the 25 ms count, or is below 10000.
