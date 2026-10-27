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
