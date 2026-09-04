"""Record raw Binance combined-stream frames to a file, one frame per line.

The recording is the fixed input for insert benchmarks, so every run (and
every writer implementation) is measured against the same real messages.

Usage: python benchmarks/record.py --seconds 120 --out benchmarks/data/sample.jsonl
"""

import argparse
import asyncio
import time
from pathlib import Path

import websockets

from tickwatch.config import load_settings


async def record(seconds: float, out: Path) -> int:
    out.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    deadline = time.monotonic() + seconds
    async with websockets.connect(load_settings().ws_url) as ws:
        with out.open("w") as f:
            while (remaining := deadline - time.monotonic()) > 0:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                except TimeoutError:
                    break
                f.write(raw if isinstance(raw, str) else raw.decode())
                f.write("\n")
                n += 1
    return n


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--seconds", type=float, default=120)
    p.add_argument("--out", type=Path, default=Path("benchmarks/data/sample.jsonl"))
    args = p.parse_args()
    n = asyncio.run(record(args.seconds, args.out))
    print(f"recorded {n} frames to {args.out}")


if __name__ == "__main__":
    main()
