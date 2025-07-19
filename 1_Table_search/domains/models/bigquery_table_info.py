

from typing import List, Dict
from dataclasses import dataclass


@dataclass
class BQTableInfo:
    dataset: str
    table_name: str
    description: str
    columns: List[Dict[str, str]]
    tags: List[str]
    last_modified: str
    row_count: int
    
    def get_full_name(self) -> str:
        return f"{self.dataset}.{self.table_name}"