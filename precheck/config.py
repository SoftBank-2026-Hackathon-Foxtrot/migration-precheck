from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Side:
    """One environment of the demo: `source` holds the app and its data, `target` waits empty."""

    role: str
    raw: dict[str, Any]

    def __getattr__(self, name: str) -> Any:
        try:
            return self.raw[name]
        except KeyError as exc:
            raise AttributeError(f"{self.role}.{name} is not configured") from exc

    def get(self, name: str, default: Any = None) -> Any:
        return self.raw.get(name, default)

    @property
    def resource_refs(self) -> dict[str, str]:
        return {"database": self.db_ref, "redis": f"resource:{self.redis_name}", "object": self.object_ref}


@dataclass(frozen=True)
class Config:
    raw: dict[str, Any]
    path: Path

    @classmethod
    def load(cls, path: str | Path) -> "Config":
        p = Path(path)
        return cls(tomllib.loads(p.read_text()), p)

    @property
    def application(self) -> str:
        return self.raw["application"]

    @property
    def platform(self) -> dict[str, Any]:
        return self.raw["platform"]

    @property
    def database(self) -> dict[str, Any]:
        return self.raw["database"]

    @property
    def cli(self) -> dict[str, Any]:
        return self.raw.get("cli", {})

    @property
    def source(self) -> Side:
        return Side("source", self.raw["source"])

    @property
    def target(self) -> Side:
        return Side("target", self.raw["target"])

    def side(self, role: str) -> Side:
        return self.source if role == "source" else self.target

    @property
    def gcp_project(self) -> str | None:
        for side in (self.source, self.target):
            if side.cloud == "gcp":
                return side.project
        return None
