from typing import Any
from .query import DataQuery


class UserProfile:
    def __init__(self):
        self._saved: dict[str, dict[str, Any]] = {}

    def save_query(self, name: str, query: DataQuery) -> None:
        state = {
            "select": getattr(query, "_select_fields"),
            "filters": getattr(query, "_filters"),
            "sort": getattr(query, "_sort_keys"),
            "group": getattr(query, "_group_field"),
        }
        self._saved[name] = state

    def run_saved(self, name: str, rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]] | list[dict[str, str]]:
        state = self._saved[name]
        q = DataQuery(rows)
        if state.get("select"):
            q.select(state["select"])
        if state.get("group"):
            q.group_by(state["group"])
        for f in state.get("sort") or []:
            q.sort_by(f[0], f[1])
        for p in state.get("filters") or []:
            q._filters.append(p)
        return q.execute()
