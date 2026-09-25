"""Prometheus metrics, served by the live consumer on METRICS_PORT (/metrics).

Latency percentiles come from histograms: the Python client's Summary type
doesn't compute quantiles, and histograms aggregate correctly across
restarts. Quantiles are therefore interpolated within bucket boundaries, so
buckets are placed around measured values:

- flush: 2-30 ms per batch in the Stage 2 benchmarks, more under backlog
- detector scoring, measured 2026-10-02 on 2,269 real BTCUSDT buckets:
  zscore p50 0.043 ms; iforest p50 1.7 ms, p99 2.0 ms, max 38 ms
"""

from prometheus_client import Counter, Gauge, Histogram

FRAMES = Counter(
    "tickwatch_frames_total", "Frames received from the websocket", ["stream"]
)  # stream: trade | depth | bookticker | other
PARSE_ERRORS = Counter("tickwatch_parse_errors_total", "Frames that failed to parse")
ROWS_WRITTEN = Counter(
    "tickwatch_rows_submitted_total", "Rows submitted to Postgres (duplicates skipped)"
)
FLUSH_SECONDS = Histogram(
    "tickwatch_flush_seconds",
    "Time to write one batch to Postgres",
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)
QUEUE_DEPTH = Gauge("tickwatch_queue_depth", "Items waiting in the writer queue")
DISCONNECTS = Counter("tickwatch_disconnects_total", "Established connections lost")
RECONNECT_ATTEMPTS = Counter(
    "tickwatch_reconnect_attempts_total", "Reconnect attempts, including failed ones"
)
CONNECTED = Gauge("tickwatch_connected", "1 while the websocket is connected")
LAST_FRAME = Gauge(
    "tickwatch_last_frame_timestamp_seconds", "Unix time the last frame was received"
)
GAPS = Counter("tickwatch_depth_gaps_total", "Depth sequence gaps", ["symbol", "cause"])
DETECTOR_SECONDS = Histogram(
    "tickwatch_detector_seconds",
    "Time to score one feature bucket",
    ["detector"],
    buckets=(0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.1),
)
FLAGS = Counter("tickwatch_anomaly_flags_total", "Anomaly flags raised", ["symbol", "detector"])
