# Detector comparison: rolling z-score vs Isolation Forest

**Data:** 3.01 hours (10,826 seconds per symbol) of BTCUSDT and ETHUSDT,
01:04-04:06 UTC on 2026-10-03, the window where both detectors were live
after the forest's one-hour warm-up. A quiet overnight market. Produced by
replaying the raw archive from 00:03 UTC through the pipeline into a scratch
database, then running `analysis/compare_detectors.py`. The full generated
output, unedited, is in
[`2026-10-03-detector-comparison.generated.md`](2026-10-03-detector-comparison.generated.md).

**Checks before reading anything into it:** re-running both detectors over
the stored features reproduces the pipeline's flags exactly (both symbols);
flag counts, same-second overlaps and the 10,826-second windows were
cross-checked with plain SQL against the replay database.

## Headline numbers

| | BTCUSDT | ETHUSDT |
|---|---:|---:|
| z-score flags (\|z\| > 4) | 189 (62.8/h, 1.75% of seconds) | 255 (84.8/h, 2.36%) |
| Isolation Forest flags (99.9th pct) | 12 (4.0/h, 0.11%) | 9 (3.0/h, 0.08%) |
| Forest flags the z-score missed | **0** | **0** |
| Jaccard overlap at production thresholds | 0.06 | 0.04 |
| Top-1% overlap at matched rates (chance = 1%) | **71%** | **49%** |
| Top-0.1% overlap at matched rates (chance = 0.1%) | 27% | 36% |
| Spearman rank correlation of scores | 0.59 | 0.34 |

## What this says

**1. At their production thresholds they mostly disagree -- and that is a
calibration difference, not a method difference.** The z-score flags 15-30x
as many seconds as the forest. Every forest flag in the window was also a
z-score flag, so the forest at its threshold behaves like a much stricter
subset. Comparing flags alone (Jaccard 0.04-0.06) would suggest the two
methods see different things; the matched-rate comparison shows that's
mostly not true.

**2. The z-score's fixed |z| > 4 is badly calibrated for this data.** Under a
normal distribution |z| > 4 happens ~0.006% of the time per feature; here it
fired on 1.75-2.36% of seconds. 71% (BTC) and 84% (ETH) of z-score flags are
driven by the absolute return. Overnight, the mid price often doesn't move
for minutes at a time, so the 5-minute baseline of returns is close to zero
and any ordinary tick move scores as extreme. The z-score assumes a roughly
stationary, roughly Gaussian baseline; zero-inflated, heavy-tailed returns
break that assumption.

**3. When forced to pick the same number of seconds, they largely agree on
BTC and partly on ETH.** 71% of each detector's top 1% of BTC seconds are
shared (vs 1% by chance); 49% on ETH. The weaker ETH agreement (rho 0.34) is
consistent with ETH's z-score picks being return spikes with an ordinary
spread (median spread percentile among z-only picks: p89), while the forest's
picks have a wide spread (p99).

**4. Where they disagree, they disagree in a recognisable way.**
- *z-score only:* one feature far outside its recent range, everything else
  ordinary -- e.g. BTC 02:12:23, z = 19.4 on a return spike with the spread
  at one tick. The forest scores these below its threshold because one
  extreme coordinate is not unusual against an hour of training data.
- *forest only:* several features moderately unusual at once, none extreme --
  typically the spread at 1.5-4 ticks together with elevated trading, z only
  ~4-5. The z-score can't see combinations; it only takes the max over
  features.

**5. The strongest agreements are market-wide moments.** The detectors run
independently per symbol, yet the top agreements for BTC and ETH include the
same two seconds, 02:42:58 and 03:15:59. At 03:15:59 the BTC spread averaged
~28x its usual one-tick spread over the second, with a large return and
heavy trading (z = 54.8). Both methods, on both assets, independently
flagging the same seconds is the most convincing evidence here that they
pick up real events rather than noise.

## What this does *not* show

- **Which detector is right.** There are no labels: nothing says which
  seconds were "truly" anomalous. This compares behaviour, not accuracy.
- **Behaviour in a busy market.** Three hours, one quiet overnight session.
  Daytime or volatile periods may look quite different, especially for the
  z-score's return feature.
- **Live behaviour across restarts.** This is a replay with continuous
  detector state. The live detectors lose their state on every restart
  (`docs/known-limitations.md`).

## What I'd change next

1. **Calibrate the z-score to a target alarm rate**, not a fixed 4 sigma:
   per-feature thresholds from rolling quantiles, or a robust z (median/MAD),
   so both detectors run at a comparable, chosen rate.
2. **Give the z-score a longer or event-aware baseline for returns**; five
   quiet minutes is too short a memory for a zero-inflated feature.
3. **Evaluate against labelled events** -- for example, seconds around known
   news releases or exchange incidents -- so the comparison can talk about
   precision, not just agreement.
4. **Re-run this report on days of data** from an always-on host.
