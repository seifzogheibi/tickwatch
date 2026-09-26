"""Container health check: healthy iff a frame arrived in the last MAX_AGE_S.

Reads the consumer's own /metrics, so it checks the thing that matters --
data flowing -- rather than just "the process exists". Exit 0 = healthy.

Usage: python -m tickwatch.healthcheck
"""

import os
import sys
import time
import urllib.request

MAX_AGE_S = 60


def main() -> int:
    port = os.environ.get("METRICS_PORT", "8000")
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=3) as r:
            body = r.read().decode()
    except OSError as e:
        print(f"unhealthy: metrics endpoint unreachable: {e}")
        return 1
    for line in body.splitlines():
        if line.startswith("tickwatch_last_frame_timestamp_seconds "):
            age = time.time() - float(line.split()[1])
            if age <= MAX_AGE_S:
                print(f"healthy: last frame {age:.1f}s ago")
                return 0
            print(f"unhealthy: last frame {age:.0f}s ago (> {MAX_AGE_S}s)")
            return 1
    print("unhealthy: no frame received yet")
    return 1


if __name__ == "__main__":
    sys.exit(main())
