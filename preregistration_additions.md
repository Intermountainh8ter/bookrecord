# Pre-registration additions

## 2026-10-27T07:33:54Z stale-level-detection proposal:ceb7c6a3448191185c08feba589912e74d1f41b712e08053ebe4d9ea344af9b7

Hypothesis: on a seeded synthetic diff stream with 200 injected dropped-delete defects that preserve the U/u chain, the update-ID chain check detects 0 of them (rate 0.0), while a snapshot comparison every 500 events and a CRC32 over the top ten levels each detect at least 0.90 of them.

Metric: /detection/rate_by_check/update_id_chain. Expected: equals 0.0.

Falsifier: the update-ID chain check detects any injected chain-preserving defect (rate above 0.0), or the snapshot or CRC32 rate falls below 0.90 under seed 7.

## 2026-10-27T07:47:57Z depth-resolved-detection proposal:923fd8e1dbbb3bf7c5de7c393dedccb4ab5ad4cebe7478a41ccf90ddf1c24d35

Hypothesis: a CRC32 over the top 10 levels catches every injected chain-clean dropped-delete defect at depth rank 1 to 10 and none at rank 11 to 50, while a top-25 checksum catches all at rank 1 to 25 and none at 26 to 50, so the top-10 catch count equals the number of trials injected at ranks 1 to 10.

Metric: /detection/catch_count_by_depth/crc32_top10. Expected: at most 20 percent of trials (ranks uniform over 1-50), expected_range [0.0, 0.25].

Falsifier: a top-10 CRC32 catch at any injected depth rank greater than 10, or a miss at any rank 1 to 10, refutes the hypothesis.
