## Detector comparison (generated)

Generated 2026-10-03 04:06 UTC by `analysis/compare_detectors.py` at commit `28f7e62` from database `tickwatch_replay`.

### BTCUSDT

10,826 seconds where both detectors scored, 2026-10-03 01:04:22 to 04:06:15 UTC (3.01 h). Re-running the detectors reproduces the stored flags exactly: **yes**.

**1. At production thresholds** (z-score: any |z| > 4; forest: above the 99.9th percentile of its own training scores)

| | z-score | Isolation Forest |
|---|---:|---:|
| flags | 189 | 12 |
| per hour | 62.8 | 4.0 |
| % of seconds | 1.75% | 0.11% |
| also flagged by the other (same second) | 12 | 12 |
| flagged only by this one | 177 | 0 |

Jaccard overlap: 0.06. Of the 177 z-only flags, 3 have a forest flag within +-2 s.

Feature with the largest |z| in each z-score flag: log_abs_return_bps 135, log_trade_count 44, spread_bps 10.

**2. At matched alarm rates** (each detector's top-k seconds by score)

| rate | k | top-k sets overlap | expected by chance |
|---|---:|---:|---:|
| 0.1% | 11 | 27% | 0.1% |
| 1.0% | 109 | 71% | 1.0% |

Spearman rank correlation of the two scores over all 10,826 seconds: **0.59**.

Median feature percentile (within this symbol's window) among each detector's top 1%:

- picked by both: log_trade_count p99, log_notional p99, spread_bps p100, log_abs_return_bps p100
- z-score only: log_trade_count p99, log_notional p98, spread_bps p98, log_abs_return_bps p99
- forest only: log_trade_count p99, log_notional p98, spread_bps p99, log_abs_return_bps p98

Strongest top-1% picks by z-score only:

- 02:12:23 z=19.4 forest=0.737 (thr 0.829): log_trade_count=5.34, log_notional=11.8, spread_bps=0.00124, log_abs_return_bps=0.582
- 02:15:36 z=14.3 forest=0.695 (thr 0.829): log_trade_count=4.98, log_notional=10.4, spread_bps=0.00118, log_abs_return_bps=0.654
- 02:06:40 z=13.9 forest=0.735 (thr 0.829): log_trade_count=4.99, log_notional=10.1, spread_bps=0.00121, log_abs_return_bps=0.786
- 01:14:06 z=13.3 forest=0.685 (thr 0.809): log_trade_count=2.3, log_notional=11.6, spread_bps=0.00118, log_abs_return_bps=1.25
- 01:46:50 z=13.0 forest=0.744 (thr 0.824): log_trade_count=6.1, log_notional=11.6, spread_bps=0.00118, log_abs_return_bps=0.865

Strongest top-1% picks by the forest only:

- 02:45:12 z=4.8 forest=0.827 (thr 0.834): log_trade_count=5.53, log_notional=10, spread_bps=0.00249, log_abs_return_bps=0.49
- 02:44:17 z=3.9 forest=0.815 (thr 0.834): log_trade_count=5.33, log_notional=10.2, spread_bps=0.00161, log_abs_return_bps=0.287
- 03:31:15 z=5.2 forest=0.812 (thr 0.836): log_trade_count=4.41, log_notional=12.4, spread_bps=0.00329, log_abs_return_bps=0.393
- 02:51:40 z=3.8 forest=0.797 (thr 0.831): log_trade_count=4.88, log_notional=11.9, spread_bps=0.00139, log_abs_return_bps=0.267
- 02:36:58 z=5.7 forest=0.797 (thr 0.834): log_trade_count=4.49, log_notional=10.7, spread_bps=0.00145, log_abs_return_bps=0.322

Strongest agreements:

- 03:05:28 z=26.2 forest=0.865 (thr 0.851): log_trade_count=6.39, log_notional=12.9, spread_bps=0.0167, log_abs_return_bps=1.57
- 03:15:59 z=54.8 forest=0.856 (thr 0.838): log_trade_count=5.95, log_notional=12.6, spread_bps=0.0336, log_abs_return_bps=1.57
- 02:42:58 z=47.3 forest=0.855 (thr 0.834): log_trade_count=5.82, log_notional=12.5, spread_bps=0.0291, log_abs_return_bps=1.35
- 02:26:14 z=10.6 forest=0.853 (thr 0.838): log_trade_count=5.83, log_notional=14, spread_bps=0.0063, log_abs_return_bps=0.994
- 01:39:49 z=20.4 forest=0.848 (thr 0.819): log_trade_count=6.08, log_notional=13.8, spread_bps=0.0132, log_abs_return_bps=1.19

### ETHUSDT

10,826 seconds where both detectors scored, 2026-10-03 01:04:22 to 04:06:15 UTC (3.01 h). Re-running the detectors reproduces the stored flags exactly: **yes**.

**1. At production thresholds** (z-score: any |z| > 4; forest: above the 99.9th percentile of its own training scores)

| | z-score | Isolation Forest |
|---|---:|---:|
| flags | 255 | 9 |
| per hour | 84.8 | 3.0 |
| % of seconds | 2.36% | 0.08% |
| also flagged by the other (same second) | 9 | 9 |
| flagged only by this one | 246 | 0 |

Jaccard overlap: 0.04. Of the 246 z-only flags, 6 have a forest flag within +-2 s.

Feature with the largest |z| in each z-score flag: log_abs_return_bps 215, log_trade_count 37, spread_bps 3.

**2. At matched alarm rates** (each detector's top-k seconds by score)

| rate | k | top-k sets overlap | expected by chance |
|---|---:|---:|---:|
| 0.1% | 11 | 36% | 0.1% |
| 1.0% | 109 | 49% | 1.0% |

Spearman rank correlation of the two scores over all 10,826 seconds: **0.34**.

Median feature percentile (within this symbol's window) among each detector's top 1%:

- picked by both: log_trade_count p98, log_notional p99, spread_bps p99, log_abs_return_bps p100
- z-score only: log_trade_count p97, log_notional p95, spread_bps p89, log_abs_return_bps p99
- forest only: log_trade_count p97, log_notional p97, spread_bps p99, log_abs_return_bps p98

Strongest top-1% picks by z-score only:

- 03:43:05 z=10.7 forest=0.673 (thr 0.795): log_trade_count=4.53, log_notional=9.58, spread_bps=0.0375, log_abs_return_bps=0.819
- 02:29:16 z=10.3 forest=0.672 (thr 0.799): log_trade_count=5.92, log_notional=8.62, spread_bps=0.0373, log_abs_return_bps=0.769
- 01:58:58 z=10.0 forest=0.633 (thr 0.803): log_trade_count=5.08, log_notional=8.93, spread_bps=0.0374, log_abs_return_bps=0.957
- 03:42:12 z=9.9 forest=0.633 (thr 0.795): log_trade_count=3.47, log_notional=5.94, spread_bps=0.0373, log_abs_return_bps=0.659
- 03:33:21 z=9.7 forest=0.668 (thr 0.791): log_trade_count=4.32, log_notional=9.45, spread_bps=0.0374, log_abs_return_bps=1.01

Strongest top-1% picks by the forest only:

- 01:16:17 z=5.5 forest=0.814 (thr 0.807): log_trade_count=5.45, log_notional=11.2, spread_bps=0.125, log_abs_return_bps=1.17
- 03:17:09 z=5.3 forest=0.810 (thr 0.794): log_trade_count=4.28, log_notional=10.8, spread_bps=0.0447, log_abs_return_bps=1.09
- 03:02:26 z=5.1 forest=0.800 (thr 0.788): log_trade_count=5.46, log_notional=12.1, spread_bps=0.0508, log_abs_return_bps=0.734
- 01:08:53 z=4.2 forest=0.795 (thr 0.806): log_trade_count=5.3, log_notional=12.4, spread_bps=0.0575, log_abs_return_bps=1.05
- 03:21:35 z=4.5 forest=0.791 (thr 0.794): log_trade_count=5.76, log_notional=9.65, spread_bps=0.0384, log_abs_return_bps=0.867

Strongest agreements:

- 03:15:59 z=14.8 forest=0.835 (thr 0.794): log_trade_count=5.46, log_notional=12.4, spread_bps=0.062, log_abs_return_bps=1.9
- 02:42:58 z=15.0 forest=0.831 (thr 0.789): log_trade_count=6.07, log_notional=13, spread_bps=0.108, log_abs_return_bps=1.71
- 01:16:12 z=8.5 forest=0.817 (thr 0.807): log_trade_count=5.76, log_notional=11, spread_bps=0.0801, log_abs_return_bps=1.49
- 02:28:22 z=6.4 forest=0.799 (thr 0.799): log_trade_count=4.11, log_notional=11.2, spread_bps=0.0492, log_abs_return_bps=0.579
- 03:43:34 z=11.5 forest=0.796 (thr 0.795): log_trade_count=5.91, log_notional=11.6, spread_bps=0.0387, log_abs_return_bps=0.985

