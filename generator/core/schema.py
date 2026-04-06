from dataclasses import dataclass
from typing import Optional


@dataclass
class ColumnSpec:
    name: str
    dtype: str
    nullable: bool = True
    description: Optional[str] = None


@dataclass
class TableSpec:
    name: str
    n_rows: int
