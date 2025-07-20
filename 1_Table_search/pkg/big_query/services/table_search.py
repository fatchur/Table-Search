
import re
import math
from typing import List, Dict

from domains.models.bigquery_table_info import BQTableInfo
from domains.services.base_bq_table_search import BaseBQSearchTable


class BQSearchTable(BaseBQSearchTable): 
    def __init__(self): 
        super().__init__()

    def add_table(self, table_info: BQTableInfo):
        table_index = len(self.tables)
        self.tables.append(table_info)
        self.process_table_keywords(table_info, table_index)

        return self
    

    def process_table_keywords(self, table_info: BQTableInfo, table_index: int):
        text_sources = [
            table_info.table_name,
            table_info.description,
            ' '.join([col.get('name', '') for col in table_info.columns]),
            ' '.join([col.get('description', '') for col in table_info.columns]),
            ' '.join(table_info.tags)
        ]
        
        for text in text_sources:
            self.extract_keywords(text)
            for keyword in self.tables_keywords:
                self.keyword_index[keyword].append(table_index)

    
    def calculate_tf_idf_score(self, query_keywords: List[str],
        table_index: int) -> float:

        table = self.tables[table_index]
        table_text = ' '.join([
            table.table_name,
            table.description,
            ' '.join([col.get('name', '') for col in table.columns]),
            ' '.join([col.get('description', '') for col in table.columns]),
            ' '.join(table.tags)
        ])
        
        table_keywords = self.extract_keywords(table_text)
        
        if not table_keywords:
            return 0.0
        
        score = 0.0
        total_tables = len(self.tables)
        
        for query_keyword in query_keywords:
            tf = table_keywords.count(query_keyword) / len(table_keywords)
            tables_with_term = len(self.keyword_index.get(query_keyword, []))
            if tables_with_term > 0:
                idf = math.log(total_tables / tables_with_term)
                score += tf * idf
        
        return score

    
    def calculate_keyword_match_score(self, query_keywords: List[str], 
        table_index: int) -> float:
        
        table = self.tables[table_index]
        table_text = ' '.join([
            table.table_name,
            table.description,
            ' '.join([col.get('name', '') for col in table.columns]),
            ' '.join([col.get('description', '') for col in table.columns]),
            ' '.join(table.tags)
        ]).lower()
        
        matches = 0
        total_query_keywords = len(query_keywords)
        
        for keyword in query_keywords:
            if keyword in table_text:
                matches += 1
        
        return matches / total_query_keywords if total_query_keywords > 0 else 0.0

    
    def calculate_combined_score(self, query_keywords: List[str], 
        table_index: int) -> float:

        tf_idf_score = self.calculate_tf_idf_score(query_keywords, table_index)
        keyword_match_score = self.calculate_keyword_match_score(query_keywords, table_index)

        return (tf_idf_score * 0.6) + (keyword_match_score * 0.4)

    
    def search(self, query: str, limit: int = 10) -> List[Dict]:
        if not query.strip():
            return []
        
        query_keywords = self.extract_keywords(query)
        
        if not query_keywords:
            return []
        
        scored_tables = []
        for i, table in enumerate(self.tables):
            score = self.calculate_combined_score(query_keywords, i)
            
            if score > 0: 
                scored_tables.append({
                    'table_name': table.get_full_name(),
                    'dataset': table.dataset,
                    'description': table.description,
                    'columns': len(table.columns),
                    'tags': table.tags,
                    'last_modified': table.last_modified,
                    'row_count': table.row_count,
                    'relevance_score': round(score, 4),
                    'matched_keywords': [kw for kw in query_keywords if kw in table.description.lower() or kw in table.table_name.lower()]
                })
    
        scored_tables.sort(key=lambda x: x['relevance_score'], reverse=True)
        
        return scored_tables[:limit]
    

    

