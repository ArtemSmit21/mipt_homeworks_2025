import csv
import json
from pathlib import Path
from typing import Any, Iterable


class StatsSerializer:
    @staticmethod
    def to_json(path: str | Path, data: Any) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    @staticmethod
    def to_csv(path: str | Path, rows: Iterable[dict[str, Any]]) -> None:
        items = list(rows)
        if not items:
            Path(path).write_text("", encoding="utf-8")
            return
        headers = list({k for r in items for k in r.keys()})
        with Path(path).open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
            for r in items:
                w.writerow({k: r.get(k) for k in headers})
