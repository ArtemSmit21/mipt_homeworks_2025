from statistics import median
from typing import Any
from .errors import FieldNotFoundError
from .utils import FieldResolver, try_int


class StatsCalculator:
    def __init__(self, rows: list[dict[str, str]]):
        self._rows = rows
        self._resolver = FieldResolver(self._infer_fields())

    def median_size(self) -> float | None:
        f = self._resolve("size")
        nums = self._collect_ints(f)
        if not nums:
            return None
        return float(median(nums))

    def most_starred_repo(self) -> dict[str, str] | None:
        f = self._resolve("stars")
        best: dict[str, str] | None = None
        best_val = -1
        for r in self._rows:
            v = r.get(f)
            iv = try_int(v) if isinstance(v, str) else None
            val = iv if iv is not None else -1
            if val > best_val:
                best = r
                best_val = val
        return best

    def repos_without_language(self) -> list[dict[str, str]]:
        f = self._resolve("language")
        out: list[dict[str, str]] = []
        for r in self._rows:
            v = r.get(f)
            if v is None or str(v).strip() == "":
                out.append(r)
        return out

    def top_commits(self, n: int = 10) -> list[dict[str, str]]:
        f = self._resolve("commits")
        sortable = []
        for r in self._rows:
            v = r.get(f)
            iv = try_int(v) if isinstance(v, str) else None
            sortable.append((iv if iv is not None else -1, r))
        sortable.sort(key=lambda x: x[0], reverse=True)
        return [r for _, r in sortable[: max(n, 0)]]

    def average_stars_by_language(self) -> list[tuple[str, float]]:
        lf = self._resolve("language")
        sf = self._resolve("stars")
        acc: dict[str, tuple[int, int]] = {}
        for r in self._rows:
            lang = r.get(lf) or ""
            v = r.get(sf)
            iv = try_int(v) if isinstance(v, str) else None
            if iv is None:
                continue
            total, cnt = acc.get(lang, (0, 0))
            acc[lang] = (total + iv, cnt + 1)
        out = []
        for lang, (total, cnt) in acc.items():
            if cnt > 0:
                out.append((lang, total / cnt))
        out.sort(key=lambda x: x[1], reverse=True)
        return out

    def _collect_ints(self, field: str) -> list[int]:
        nums: list[int] = []
        for r in self._rows:
            v = r.get(field)
            if isinstance(v, str):
                iv = try_int(v)
                if iv is not None:
                    nums.append(iv)
        return nums

    def _resolve(self, field: str) -> str:
        try:
            return self._resolver.resolve(field)
        except KeyError as e:
            candidates = tuple(str(e).split(",")) if str(e) else ()
            raise FieldNotFoundError(field, candidates)

    def _infer_fields(self) -> set[str]:
        fields: set[str] = set()
        for r in self._rows[:5]:
            fields.update(r.keys())
        return fields
