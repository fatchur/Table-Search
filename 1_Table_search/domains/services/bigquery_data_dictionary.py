

import re
from typing import List, Dict, Any
from collections import defaultdict
from abc import ABC, abstractmethod

from domains.models.bigquery_table_info import BQTableInfo

class BQDataDictionary(ABC): 
    def __init__(self):
        self.tables: List[BQTableInfo] = []
        self.tables_keywords: list[Any] = []
        self.keyword_index: Dict[str, List[int]] = defaultdict(list)
        self.stop_words = {
            'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from',
            'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on', 'that', 'the',
            'to', 'was', 'were', 'will', 'with', 'where', 'can', 'find', 'i'
        }
    
    def extract_keywords(self, text: str):
        if not text:
            return []
        
        words = re.findall(r'\b[a-zA-Z]+\b', text.lower())
        self.tables_keywords = [word for word in words if word not in self.stop_words and len(word) > 2]
        
        return self.tables_keywords
    
    @abstractmethod
    def add_table(self, table_info: BQTableInfo):
        pass
    
    @abstractmethod
    def process_table_keywords(self, table_info: BQTableInfo, table_index: int):
        pass

    @abstractmethod
    def search(self, query: str, limit: int = 10) -> List[Dict]:
        pass

