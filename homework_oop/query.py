from typing import Callable, Iterable
from .errors import FieldNotFoundError, InvalidOperationError
from .utils import FieldResolver, try_float, try_int


class DataQuery:
    def __init__(self, rows: list[dict[str, str]]):
        self._rows = list(rows)
        self._filters: list[Callable[[dict[str, str]], bool]] = []
        self._select_fields: list[str] | None = None
        self._sort_keys: list[tuple[str, bool]] = []
        self._group_field: str | None = None
        self._resolver = FieldResolver(self._infer_fields())

    def select(self, fields: Iterable[str]) -> "DataQuery":
        resolved = [self._resolve(x) for x in fields]
        self._select_fields = resolved
        return self

    def where_equal(self, field: str, value: str) -> "DataQuery":
        f = self._resolve(field)
        self._filters.append(lambda r: r.get(f) == value)
        return self

    def where_in(self, field: str, values: Iterable[str]) -> "DataQuery":
        f = self._resolve(field)
        allowed = set(values)
        self._filters.append(lambda r: r.get(f) in allowed)
        return self

    def sort_by(self, field: str, descending: bool = False) -> "DataQuery":
        f = self._resolve(field)
        self._sort_keys.append((f, descending))
        return self

    def group_by(self, field: str) -> "DataQuery":
        f = self._resolve(field)
        self._group_field = f
        return self

    def execute(self) -> dict[str, list[dict[str, str]]] | list[dict[str, str]]:
        rows = self._apply_filters(self._rows)
        rows = self._apply_select(rows)
        rows = self._apply_sort(rows)
        if self._group_field is not None:
            return self._apply_group(rows, self._group_field)
        return rows

    def _apply_filters(self, rows: list[dict[str, str]]) -> list[dict[str, str]]:
        if not self._filters:
            return rows
        out = rows
        for predicate in self._filters:
            out = [r for r in out if predicate(r)]
        return out

    def _apply_select(self, rows: list[dict[str, str]]) -> list[dict[str, str]]:
        if not self._select_fields:
            return rows
        selected = []
        for r in rows:
            selected.append({f: r.get(f) for f in self._select_fields})
        return selected

    def _apply_sort(self, rows: list[dict[str, str]]) -> list[dict[str, str]]:
        if not self._sort_keys:
            return rows
        keys = list(self._sort_keys)

        def key_fn(r: dict[str, str]) -> tuple:
            out: list[object] = []
            for f, _rev in keys:
                v = r.get(f)
                if isinstance(v, str):
                    iv = try_int(v)
                    if iv is not None:
                        out.append(iv)
                        continue
                    fv = try_float(v)
                    if fv is not None:
                        out.append(fv)
                        continue
                out.append(v)
            return tuple(out)

        primary_rev = keys[0][1]
        sorted_rows = sorted(rows, key=key_fn, reverse=primary_rev)
        if len(keys) > 1:
            for f, rev in keys[1:][::-1]:
                sorted_rows = sorted(sorted_rows, key=lambda r: r.get(f), reverse=rev)
        return sorted_rows

    def _apply_group(self, rows: list[dict[str, str]], field: str) -> dict[str, list[dict[str, str]]]:
        groups: dict[str, list[dict[str, str]]] = {}
        for r in rows:
            key = r.get(field)
            if key is None:
                key = ""
            groups.setdefault(str(key), []).append(r)
        return groups

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
