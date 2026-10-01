"""Generate ops/grafana/dashboards/tickwatch.json.

The dashboard is defined here, in code, and the JSON is generated output that
Grafana provisions from. Edit this file, then run:

    python ops/grafana/build_dashboard.py

tests/test_dashboard.py fails if the committed JSON is out of date.
"""

import json
from pathlib import Path

OUT = Path(__file__).parent / "dashboards" / "tickwatch.json"

PROM = {"type": "prometheus", "uid": "prometheus"}
TSDB = {"type": "grafana-postgresql-datasource", "uid": "timescaledb"}


def prom(expr: str, legend: str = "") -> dict:
    return {"datasource": PROM, "expr": expr, "legendFormat": legend, "refId": legend or "A"}


def sql(query: str, fmt: str = "time_series", ref: str = "A") -> dict:
    return {
        "datasource": TSDB,
        "editorMode": "code",
        "format": fmt,
        "rawQuery": True,
        "rawSql": " ".join(query.split()),
        "refId": ref,
    }


_next_id = iter(range(1, 1000))


def panel(kind: str, title: str, pos: tuple[int, int, int, int], targets: list[dict], *,
          unit: str = "short", description: str = "", thresholds: list | None = None,
          mappings: list | None = None, options: dict | None = None,
          datasource: dict = PROM, decimals: int | None = None,
          no_value: str | None = None) -> dict:  # fmt: skip
    x, y, w, h = pos
    defaults: dict = {"unit": unit}
    if decimals is not None:
        defaults["decimals"] = decimals
    if no_value is not None:
        defaults["noValue"] = no_value
    if thresholds:
        defaults["thresholds"] = {"mode": "absolute", "steps": thresholds}
        defaults["color"] = {"mode": "thresholds"}
    if mappings:
        defaults["mappings"] = mappings
    if kind == "stat" and unit == "short":
        defaults["decimals"] = 0  # counts: increase() extrapolates, so 1.00 -> 1
    if kind == "stat":
        # "last", not Grafana's default "lastNotNull": a dead series must not
        # keep showing its final healthy value. Instant queries for the same reason.
        options = {"reduceOptions": {"calcs": ["last"], "fields": "", "values": False},
                   **(options or {})}  # fmt: skip
        targets = [{**t, "instant": True, "range": False} for t in targets]
    return {
        "id": next(_next_id),
        "type": kind,
        "title": title,
        "description": description,
        "datasource": datasource,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": targets,
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": options or {},
    }


def row(title: str, y: int) -> dict:
    return {
        "id": next(_next_id),
        "type": "row",
        "title": title,
        "collapsed": False,
        "gridPos": {"x": 0, "y": y, "w": 24, "h": 1},
        "panels": [],
    }


def quantiles(metric: str, by: str = "") -> list[dict]:
    group = f"le{', ' + by if by else ''}"
    label = f"{{{{{by}}}}} " if by else ""
    return [
        prom(
            f"histogram_quantile({q}, sum by ({group}) (rate({metric}_bucket[5m])))",
            f"{label}p{int(q * 100)}",
        )
        for q in (0.5, 0.95, 0.99)
    ]


GREEN, AMBER, RED = "green", "orange", "red"
MSG_S = "suffix: msg/s"


def build() -> dict:
    panels = [
        row("Health", 0),
        panel("stat", "Websocket", (0, 1, 4, 4),
              # DOWN if the consumer reports disconnected *or* can't be scraped.
              [prom('(max(tickwatch_connected) * max(up{job="tickwatch"})) or vector(0)')],
              mappings=[{"type": "value", "options": {
                  "0": {"text": "DOWN", "color": RED}, "1": {"text": "UP", "color": GREEN}}}],
              thresholds=[{"color": RED, "value": None}, {"color": GREEN, "value": 1}]),
        panel("stat", "Seconds since last frame", (4, 1, 4, 4),
              # max_over_time keeps the last timestamp after the consumer
              # stops being scraped, so this keeps rising instead of freezing.
              [prom("time() - max(max_over_time(tickwatch_last_frame_timestamp_seconds[1h]))")],
              unit="s",
              description="Catches a silent stall that the connection flag would miss. "
                          "Reads 0-5 s when healthy: Prometheus scrapes every 5 s.",
              thresholds=[{"color": GREEN, "value": None}, {"color": AMBER, "value": 15},
                          {"color": RED, "value": 60}]),
        panel("stat", "Messages/s", (8, 1, 4, 4),
              [prom("sum(rate(tickwatch_frames_total[1m]))")], unit=MSG_S,
              description="All streams. ~120-150/s for two symbols on a quiet day.",
              thresholds=[{"color": RED, "value": None}, {"color": GREEN, "value": 1}]),
        panel("stat", "Writer queue", (12, 1, 4, 4), [prom("max(tickwatch_queue_depth)")],
              description="Items waiting to be written. Sustained growth = DB falling behind.",
              thresholds=[{"color": GREEN, "value": None}, {"color": AMBER, "value": 1000},
                          {"color": RED, "value": 8000}]),
        panel("stat", "Disconnects (24h)", (16, 1, 4, 4),
              [prom("sum(increase(tickwatch_disconnects_total[24h])) or vector(0)")],
              description="Binance closes every connection after 24 h, so ~1/day is normal.",
              thresholds=[{"color": GREEN, "value": None}, {"color": AMBER, "value": 3}]),
        panel("stat", "Depth gaps (24h)", (20, 1, 4, 4),
              [prom("sum(increase(tickwatch_depth_gaps_total[24h])) or vector(0)")],
              description="Breaks in U/u sequencing. Each reconnect causes one per symbol.",
              thresholds=[{"color": GREEN, "value": None}, {"color": AMBER, "value": 5}]),

        row("Ingestion", 5),
        panel("timeseries", "Messages/s by stream", (0, 6, 12, 8),
              [prom("sum by (stream) (rate(tickwatch_frames_total[1m]))", "{{stream}}")],
              unit=MSG_S),
        panel("timeseries", "Insert latency (per flush)", (12, 6, 12, 8),
              quantiles("tickwatch_flush_seconds"), unit="s",
              description="Time to write one batch (COPY + merge, then low-volume inserts). "
                          "Quantiles are interpolated within histogram buckets."),
        panel("timeseries", "Writer queue depth", (0, 14, 8, 7),
              [prom("max(tickwatch_queue_depth)", "queued")]),
        panel("timeseries", "Rows submitted/s", (8, 14, 8, 7),
              [prom("sum(rate(tickwatch_rows_submitted_total[1m]))", "rows/s")],
              unit="suffix: rows/s"),
        panel("timeseries", "Disconnects, reconnect attempts, gaps, parse errors", (16, 14, 8, 7), [
            prom("sum(increase(tickwatch_disconnects_total[5m]))", "disconnects"),
            prom("sum(increase(tickwatch_reconnect_attempts_total[5m]))", "reconnect attempts"),
            prom("sum by (cause) (increase(tickwatch_depth_gaps_total[5m]))", "gaps ({{cause}})"),
            prom("sum(increase(tickwatch_parse_errors_total[5m]))", "parse errors"),
        ], description="Counts per 5 minutes."),

        row("Anomaly detection", 21),
        panel("timeseries", "Detector scoring time", (0, 22, 12, 8),
              quantiles("tickwatch_detector_seconds", "detector"), unit="s",
              description="Per feature bucket. Isolation Forest is ~40x slower than the "
                          "z-score (sklearn per-call overhead on a single row)."),
        panel("timeseries", "Flags per hour", (12, 22, 12, 8),
              [prom("sum by (detector, symbol) (increase(tickwatch_anomaly_flags_total[1h]))",
                    "{{detector}} {{symbol}}")]),
        btc_mid := panel("timeseries", "BTCUSDT mid", (0, 30, 12, 8), [sql(
            """SELECT $__timeGroupAlias(time, $__interval, NULL), avg(mid) AS "BTCUSDT"
               FROM features_1s WHERE symbol = 'BTCUSDT' AND $__timeFilter(time)
               GROUP BY 1 ORDER BY 1""")], unit="prefix:$", decimals=0, datasource=TSDB,
              description="Red markers: Isolation Forest flags."),
        eth_mid := panel("timeseries", "ETHUSDT mid", (12, 30, 12, 8), [sql(
            """SELECT $__timeGroupAlias(time, $__interval, NULL), avg(mid) AS "ETHUSDT"
               FROM features_1s WHERE symbol = 'ETHUSDT' AND $__timeFilter(time)
               GROUP BY 1 ORDER BY 1""")], unit="prefix:$", decimals=2, datasource=TSDB,
              description="Red markers: Isolation Forest flags."),
        panel("timeseries", "Spread (bps, time-weighted)", (0, 38, 12, 8), [sql(
            """SELECT $__timeGroupAlias(time, $__interval, NULL), symbol AS metric,
                      max(spread_bps) AS value
               FROM features_1s WHERE $__timeFilter(time) AND spread_bps IS NOT NULL
               GROUP BY 1, 2 ORDER BY 1""")], datasource=TSDB,
              description="Max of per-second spreads in each interval, so brief widenings "
                          "stay visible when zoomed out."),
        panel("table", "Recent anomaly flags", (12, 38, 12, 8), [sql(
            """SELECT time, symbol, detector, round(score::numeric, 2) AS score,
                      round(threshold::numeric, 2) AS threshold, features::text AS features
               FROM anomaly_flags WHERE $__timeFilter(time)
               ORDER BY time DESC LIMIT 200""", fmt="table")], datasource=TSDB),
        panel("table", "Depth gaps", (0, 46, 24, 6), [sql(
            """SELECT time, symbol, cause, missing_update_ids, prev_final_update_id,
                      first_update_id
               FROM depth_gaps WHERE $__timeFilter(time) ORDER BY time DESC LIMIT 100""",
            fmt="table")], datasource=TSDB, no_value="No gaps in range"),
    ]  # fmt: skip
    return {
        "uid": "tickwatch",
        "title": "tickwatch",
        "tags": ["tickwatch"],
        "timezone": "utc",
        "schemaVersion": 39,
        "version": 1,
        "editable": False,
        "refresh": "10s",
        "time": {"from": "now-3h", "to": "now"},
        "annotations": {
            "list": [
                {
                    # Forest flags only: the z-score fires 60-85 times an hour
                    # (docs/reports/2026-10-03-detector-comparison.md), which
                    # turns the price panels into a solid wall of markers.
                    "name": "Isolation Forest flags",
                    "datasource": TSDB,
                    "enable": True,
                    "iconColor": "rgba(255, 96, 96, 1)",
                    # Only on the price panels: on ingestion charts they're noise.
                    "filter": {"exclude": False, "ids": [btc_mid["id"], eth_mid["id"]]},
                    "target": sql(
                        """SELECT time, detector || ' ' || symbol || ' score '
                                  || round(score::numeric, 2) AS text,
                                  detector AS tags
                           FROM anomaly_flags
                           WHERE detector = 'iforest' AND $__timeFilter(time)""",
                        fmt="table",
                    ),
                }
            ]
        },
        "panels": panels,
    }


def render() -> str:
    global _next_id
    _next_id = iter(range(1, 1000))
    return json.dumps(build(), indent=2) + "\n"


if __name__ == "__main__":
    OUT.write_text(render())
    print(f"wrote {OUT}")
