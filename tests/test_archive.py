import asyncio
import gzip
from datetime import UTC, datetime, timedelta
from pathlib import Path

from tickwatch.archive import CONNECTED_MARKER, RawArchive, iter_archive

T = datetime(2026, 10, 2, 13, 59, 59, 999_999, tzinfo=UTC)


def write_all(root: Path, records: list[tuple[datetime, str | bytes]]) -> None:
    async def go() -> None:
        a = RawArchive(root)
        for t, payload in records:
            a.write(t, payload)
        await a.close()

    asyncio.run(go())


def test_round_trip_preserves_payload_and_exact_timestamp(tmp_path: Path) -> None:
    records = [(T, CONNECTED_MARKER), (T, '{"a":1}'), (T, b'{"b":2}')]
    write_all(tmp_path, records)
    assert list(iter_archive([tmp_path])) == [
        (T, CONNECTED_MARKER),
        (T, '{"a":1}'),
        (T, '{"b":2}'),  # bytes frames come back as text
    ]


def test_hour_rollover_compresses_previous_hour_and_reads_in_order(tmp_path: Path) -> None:
    later = T + timedelta(microseconds=1)  # 14:00:00.000000
    write_all(tmp_path, [(T, '{"n":1}'), (later, '{"n":2}')])
    day = tmp_path / "2026-10-02"
    assert sorted(p.name for p in day.iterdir()) == ["13.tsv.gz", "14.tsv"]
    assert [p for _, p in iter_archive([tmp_path])] == ['{"n":1}', '{"n":2}']


def test_restart_within_the_hour_appends(tmp_path: Path) -> None:
    write_all(tmp_path, [(T, '{"n":1}')])
    write_all(tmp_path, [(T, '{"n":2}')])
    assert [p for _, p in iter_archive([tmp_path])] == ['{"n":1}', '{"n":2}']


def test_compress_stale_handles_hours_left_by_a_crash(tmp_path: Path) -> None:
    stale = tmp_path / "2026-10-01" / "23.tsv"
    stale.parent.mkdir(parents=True)
    stale.write_text('1790000000000000000\t{"n":0}\n')

    async def go() -> None:
        a = RawArchive(tmp_path)
        a.compress_stale()
        await a.close()

    asyncio.run(go())
    assert not stale.exists()
    assert gzip.open(stale.with_suffix(".tsv.gz"), "rt").read() == '1790000000000000000\t{"n":0}\n'


def test_malformed_lines_are_skipped(tmp_path: Path) -> None:
    f = tmp_path / "2026-10-02" / "13.tsv"
    f.parent.mkdir(parents=True)
    f.write_text(
        '1790949599999999000\t{"ok":1}\n'
        "garbage line\n"
        "\t{}\n"
        "17909495999\n"  # cut short by a crash mid-write
        '1790949599999999000\t{"ok":2}\n'
    )
    assert [p for _, p in iter_archive([f])] == ['{"ok":1}', '{"ok":2}']


def test_files_and_directories_mix_without_duplicates(tmp_path: Path) -> None:
    write_all(tmp_path, [(T, '{"n":1}')])
    f = tmp_path / "2026-10-02" / "13.tsv"
    assert len(list(iter_archive([tmp_path, f]))) == 1


def test_multiple_roots_are_read_chronologically(tmp_path: Path) -> None:
    # e.g. today's local archive plus yesterday's copied off the server. Root
    # names sort alphabetically opposite to time, so path order would be wrong.
    local, copied = tmp_path / "a_local", tmp_path / "b_copied"
    write_all(local, [(T, '{"day":2}')])
    write_all(copied, [(T - timedelta(days=1), '{"day":1}')])
    assert [p for _, p in iter_archive([local, copied])] == ['{"day":1}', '{"day":2}']


def test_same_hour_in_two_roots_is_merged_by_time(tmp_path: Path) -> None:
    # Regression, 2026-10-03: the host archive held 01:00-01:11 in 01.tsv and
    # the container volume 01:12 onwards, also in 01.tsv. Concatenating
    # same-hour files in arbitrary order replayed time backwards. Root names
    # here sort opposite to time, and the second file interleaves the first.
    t = datetime(2026, 10, 3, 1, 0, tzinfo=UTC)
    host, volume = tmp_path / "b_host", tmp_path / "a_volume"
    write_all(host, [(t, '{"n":1}'), (t + timedelta(minutes=5), '{"n":3}')])
    write_all(
        volume, [(t + timedelta(minutes=2), '{"n":2}'), (t + timedelta(minutes=12), '{"n":4}')]
    )
    replayed = list(iter_archive([host, volume]))
    assert [p for _, p in replayed] == ['{"n":1}', '{"n":2}', '{"n":3}', '{"n":4}']
    times = [ts for ts, _ in replayed]
    assert times == sorted(times)
