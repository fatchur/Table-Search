


class DistanceCost(): 
    def __init__(self, tables: List[str]): 
        self.tables = tables

    def calculate_tf_idf_score(self, query_keywords: List[str], table_index: int) -> float:
        table = self.tables[table_index]
        
        table_text = ' '.join([
            table.table_name,
            table.description,
            ' '.join([col.get('name', '') for col in table.columns]),
            ' '.join([col.get('description', '') for col in table.columns]),
            ' '.join(table.tags)
        ])
        
        table_keywords = self._extract_keywords(table_text)
        
        if not table_keywords:
            return 0.0
        
        # Calculate TF-IDF score
        score = 0.0
        total_tables = len(self.tables)
        
        for query_keyword in query_keywords:
            # Term frequency in the table
            tf = table_keywords.count(query_keyword) / len(table_keywords)
            
            # Inverse document frequency
            tables_with_term = len(self.keyword_index.get(query_keyword, []))
            if tables_with_term > 0:
                idf = math.log(total_tables / tables_with_term)
                score += tf * idf
        
        return score
    
    def _calculate_keyword_match_score(self, query_keywords: List[str], table_index: int) -> float:
        """Calculate keyword matching score"""
        table = self.tables[table_index]
        
        # Get all text from the table
        table_text = ' '.join([
            table.table_name,
            table.description,
            ' '.join([col.get('name', '') for col in table.columns]),
            ' '.join([col.get('description', '') for col in table.columns]),
            ' '.join(table.tags)
        ]).lower()
        
        # Calculate match score
        matches = 0
        total_query_keywords = len(query_keywords)
        
        for keyword in query_keywords:
            if keyword in table_text:
                matches += 1
        
        return matches / total_query_keywords if total_query_keywords > 0 else 0.0
    
    def _calculate_combined_score(self, query_keywords: List[str], table_index: int) -> float:
        """Calculate combined relevance score"""
        tf_idf_score = self._calculate_tf_idf_score(query_keywords, table_index)
        keyword_match_score = self._calculate_keyword_match_score(query_keywords, table_index)
        
        # Weighted combination of scores
        return (tf_idf_score * 0.6) + (keyword_match_score * 0.4)
    
    def search(self, query: str, limit: int = 10) -> List[Dict]:
        """
        Search for relevant tables based on a natural language query
        
        Args:
            query: Natural language query
            limit: Maximum number of results to return
            
        Returns:
            List of dictionaries containing table information and relevance scores
        """
        if not query.strip():
            return []
        
        # Extract keywords from query
        query_keywords = self._extract_keywords(query)
        
        if not query_keywords:
            return []
        
        # Calculate scores for all tables
        scored_tables = []
        
        for i, table in enumerate(self.tables):
            score = self._calculate_combined_score(query_keywords, i)
            
            if score > 0:  # Only include tables with positive scores
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
        
        # Sort by relevance score (descending) and return top results
        scored_tables.sort(key=lambda x: x['relevance_score'], reverse=True)
        
        return scored_tables[:limit]