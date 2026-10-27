# Pre-registration additions

## 2026-10-27T07:33:54Z stale-level-detection proposal:ceb7c6a3448191185c08feba589912e74d1f41b712e08053ebe4d9ea344af9b7

Hypothesis: on a seeded synthetic diff stream with 200 injected dropped-delete defects that preserve the U/u chain, the update-ID chain check detects 0 of them (rate 0.0), while a snapshot comparison every 500 events and a CRC32 over the top ten levels each detect at least 0.90 of them.

Metric: /detection/rate_by_check/update_id_chain. Expected: equals 0.0.

Falsifier: the update-ID chain check detects any injected chain-preserving defect (rate above 0.0), or the snapshot or CRC32 rate falls below 0.90 under seed 7.
