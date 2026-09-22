import random

from tickwatch.backoff import Backoff


def test_delays_stay_within_the_exponential_ceiling() -> None:
    b = Backoff(base_s=1.0, cap_s=60.0, rng=random.Random(42))
    for n in range(40):
        ceiling = min(60.0, 2.0**n)
        assert 0 <= b.next_delay() <= ceiling


def test_ceiling_grows_then_caps() -> None:
    # Sample many delays per attempt: the max seen approaches the ceiling.
    def max_delay_at(attempt: int) -> float:
        rng = random.Random(0)
        best = 0.0
        for _ in range(500):
            b = Backoff(rng=rng)
            b.attempt = attempt
            best = max(best, b.next_delay())
        return best

    assert max_delay_at(0) <= 1.0
    assert 3.5 < max_delay_at(2) <= 4.0
    assert 55 < max_delay_at(10) <= 60.0  # 2**10 = 1024, capped at 60


def test_reset_starts_the_schedule_over() -> None:
    b = Backoff(rng=random.Random(1))
    for _ in range(10):
        b.next_delay()
    b.reset()
    assert b.attempt == 0
    assert b.next_delay() <= 1.0


def test_very_long_outage_does_not_overflow() -> None:
    # Regression: base * 2**n overflowed a float past n ~ 1024.
    b = Backoff()
    b.attempt = 10_000
    assert 0 <= b.next_delay() <= 60.0


def test_jitter_spreads_clients_out() -> None:
    delays = {round(Backoff(rng=random.Random(s)).next_delay(), 6) for s in range(20)}
    assert len(delays) == 20
