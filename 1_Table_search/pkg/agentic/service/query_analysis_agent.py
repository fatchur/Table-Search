
import re
from typing import List, Dict

from domains.values.agent_status import AgentStatus
from domains.models.agent_task import AgentTask
from domains.services.base_agent import BaseAgent

from domains.values.constant.agent_query_analysis_capability import AGENT_QUERY_ANALYSIS_CAPABILITY
from domains.values.constant.agent_intent_pattern import AGENT_INTENT_PATTERN


class QueryAnalysisAgent(BaseAgent):
    
    def __init__(self):
        super().__init__(
            name="QueryAnalyzer",
            capabilities=AGENT_QUERY_ANALYSIS_CAPABILITY)
            
        self.intent_patterns = AGENT_INTENT_PATTERN
    
    async def process_task(self, task: AgentTask) -> AgentTask:
        self.status = AgentStatus.WORKING
        
        try:
            query = task.input_data.get("query", "")
            intent = self._classify_intent(query)
            keywords = self._extract_keywords(query)
            entities = self._extract_entities(query)
            
            sub_queries = self._decompose_query(query, intent)
            
            task.output_data = {
                "original_query": query,
                "intent": intent,
                "keywords": keywords,
                "entities": entities,
                "sub_queries": sub_queries,
                "confidence": self._calculate_confidence(query, intent)
            }
            
            task.status = AgentStatus.COMPLETED
            self.logger.info(f"Query analyzed: {intent} intent with {len(keywords)} keywords")
            
        except Exception as e:
            task.status = AgentStatus.FAILED
            task.output_data = {"error": str(e)}
            self.logger.error(f"Query analysis failed: {e}")
        
        self.status = AgentStatus.IDLE
        return task

    
    def _classify_intent(self, query: str) -> str:
        query_lower = query.lower()
        
        intent_scores = {}
        for intent, patterns in self.intent_patterns.items():
            score = sum(1 for pattern in patterns if pattern in query_lower)
            if score > 0:
                intent_scores[intent] = score
        
        if intent_scores:
            return max(intent_scores, key=lambda x: intent_scores[x])

        return "general_search"

    
    def _extract_keywords(self, query: str) -> List[str]:
        """Extract meaningful keywords from query"""
        stop_words = {
            'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from',
            'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on', 'that', 'the',
            'to', 'was', 'were', 'will', 'with', 'where', 'can', 'find', 'i'
        }
        
        words = re.findall(r'\b[a-zA-Z]+\b', query.lower())
        keywords = [word for word in words if word not in stop_words and len(word) > 2]
        
        return keywords
    
    def _extract_entities(self, query: str) -> Dict[str, List[str]]:
        entities = {
            "datasets": [],
            "table_names": [],
            "time_references": [],
            "metrics": []
        }
        
        query_lower = query.lower()
        if "marketing" in query_lower:
            entities["datasets"].append("marketing")
        if "sales" in query_lower:
            entities["datasets"].append("sales")
        if "analytics" in query_lower:
            entities["datasets"].append("analytics")
        
        time_words = ["daily", "weekly", "monthly", "yearly", "today", "yesterday"]
        entities["time_references"] = [word for word in time_words if word in query_lower]
        
        metric_words = ["revenue", "performance", "clicks", "impressions", "conversions"]
        entities["metrics"] = [word for word in metric_words if word in query_lower]
        
        return entities

    
    def _decompose_query(self, query: str, intent: str) -> List[str]:
        if "and" in query.lower() or "," in query:
            parts = query.replace(" and ", ",").split(",")
            return [part.strip() for part in parts if part.strip()]
        
        return [query]

    
    def _calculate_confidence(self, query: str, intent: str) -> float:
        base_confidence = 0.7
        
        domain_terms = ["table", "dataset", "column", "data", "bigquery"]
        domain_matches = sum(1 for term in domain_terms if term in query.lower())
        
        confidence = min(base_confidence + (domain_matches * 0.1), 1.0)
        return round(confidence, 2)