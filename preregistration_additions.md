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
