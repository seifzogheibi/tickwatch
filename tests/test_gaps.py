from tickwatch.gaps import GapDetector

from .helpers import depth


def test_contiguous_sequence_has_no_gaps() -> None:
    d = GapDetector()
    assert d.check(depth("BTCUSDT", 100, 110)) is None  # first update: nothing to compare
    assert d.check(depth("BTCUSDT", 111, 120)) is None
    assert d.check(depth("BTCUSDT", 121, 121)) is None  # single-ID update


def test_dropped_update_reports_missing_id_count() -> None:
    d = GapDetector()
    d.check(depth("BTCUSDT", 100, 110))
    # 111..120 never arrived
    gap = d.check(depth("BTCUSDT", 121, 130))
    assert gap is not None
    assert (gap.prev_final_update_id, gap.first_update_id) == (110, 121)
    assert gap.missing_update_ids == 10
    assert gap.cause == "stream"


def test_repeat_or_overlap_is_a_negative_gap() -> None:
    d = GapDetector()
    d.check(depth("BTCUSDT", 100, 110))
    gap = d.check(depth("BTCUSDT", 100, 110))  # same update again
    assert gap is not None and gap.missing_update_ids == -11


def test_detection_continues_from_the_latest_update_after_a_gap() -> None:
    d = GapDetector()
    d.check(depth("BTCUSDT", 100, 110))
    d.check(depth("BTCUSDT", 121, 130))  # gap
    assert d.check(depth("BTCUSDT", 131, 140)) is None  # contiguous with the new position


def test_symbols_are_tracked_independently() -> None:
    d = GapDetector()
    d.check(depth("BTCUSDT", 100, 110))
    d.check(depth("ETHUSDT", 5000, 5010))
    assert d.check(depth("ETHUSDT", 5011, 5020)) is None
    assert d.check(depth("BTCUSDT", 111, 120)) is None
    gap = d.check(depth("ETHUSDT", 5030, 5040))
    assert gap is not None and gap.symbol == "ETHUSDT"


def test_reconnect_attributes_only_the_next_break_per_symbol() -> None:
    d = GapDetector()
    d.check(depth("BTCUSDT", 100, 110))
    d.check(depth("ETHUSDT", 5000, 5010))
    d.mark_reconnect()
    btc = d.check(depth("BTCUSDT", 500, 510))
    eth = d.check(depth("ETHUSDT", 9000, 9010))
    assert (btc.cause, eth.cause) == ("reconnect", "reconnect")
    # A later break on the same connection is the stream's fault again.
    assert d.check(depth("BTCUSDT", 600, 610)).cause == "stream"


def test_reconnect_without_a_break_does_not_taint_a_later_gap() -> None:
    d = GapDetector()
    d.check(depth("BTCUSDT", 100, 110))
    d.mark_reconnect()
    assert d.check(depth("BTCUSDT", 111, 120)) is None  # resumed seamlessly
    assert d.check(depth("BTCUSDT", 200, 210)).cause == "stream"


def test_symbol_first_seen_after_reconnect_is_not_a_gap() -> None:
    d = GapDetector()
    d.check(depth("BTCUSDT", 100, 110))
    d.mark_reconnect()
    assert d.check(depth("ETHUSDT", 5000, 5010)) is None
