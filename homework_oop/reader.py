import csv
from pathlib import Path
from importlib.resources import files
from typing import Iterable

class CsvRepositoryReader:
    def __init__(self, path: str | Path, encoding: str = "utf-8"):
        self._path = Path(path)
        self._encoding = encoding

    @classmethod
    def FromPackageCsv(cls, package: str = "homework_oop", name: str = "repositories.csv"):
        return cls(files(package).joinpath(name))

    def read(self) -> list[dict[str, str]]:
        text = self._path.read_text(encoding=self._encoding)
        dialect = self._sniff(text)
        rows: list[dict[str, str]] = []
        for row in csv.DictReader(text.splitlines(), dialect=dialect):
            rows.append({k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()})
        return rows

    def _sniff(self, text: str) -> csv.Dialect:
        try:
            return csv.Sniffer().sniff(text, delimiters=",;\t|")
        except Exception:
            return csv.get_dialect("excel")
