"""Exponential backoff with full jitter for reconnect attempts."""

import random


class Backoff:
    """Delay before reconnect attempt n is uniform in [0, min(cap, base * 2**n)].

    Full jitter (rather than a fixed exponential schedule) spreads retries out,
    so many clients dropped by the same outage don't reconnect in lockstep.
    Call `reset()` once a connection has proved healthy, so one blip after a
    day of uptime doesn't inherit the delay from an outage hours ago.
    """

    def __init__(
        self, base_s: float = 1.0, cap_s: float = 60.0, rng: random.Random | None = None
    ) -> None:
        self.base_s = base_s
        self.cap_s = cap_s
        self.attempt = 0
        self._rng = rng or random.Random()

    def next_delay(self) -> float:
        ceiling = min(self.cap_s, self.base_s * 2**self.attempt)
        self.attempt += 1
        return self._rng.uniform(0, ceiling)

    def reset(self) -> None:
        self.attempt = 0
