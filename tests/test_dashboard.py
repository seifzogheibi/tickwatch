import importlib.util
from pathlib import Path

ROOT = Path(__file__).parent.parent
spec = importlib.util.spec_from_file_location(
    "build_dashboard", ROOT / "ops" / "grafana" / "build_dashboard.py"
)
build_dashboard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(build_dashboard)


def test_committed_dashboard_json_matches_generator() -> None:
    # Run `python ops/grafana/build_dashboard.py` after editing the dashboard.
    assert build_dashboard.OUT.read_text() == build_dashboard.render()


def test_panels_do_not_overlap() -> None:
    cells = set()
    for p in build_dashboard.build()["panels"]:
        g = p["gridPos"]
        for x in range(g["x"], g["x"] + g["w"]):
            for y in range(g["y"], g["y"] + g["h"]):
                assert (x, y) not in cells, f"{p['title']} overlaps at {(x, y)}"
                cells.add((x, y))
        assert g["x"] + g["w"] <= 24, f"{p['title']} is wider than the grid"
