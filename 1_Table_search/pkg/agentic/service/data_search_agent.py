

from typing import List, Dict
from datetime import datetime

from domains.values.agent_status import AgentStatus
from domains.models.agent_task import AgentTask
from domains.services.base_agent import BaseAgent
from pkg.big_query.services.table_search import BQSearchTable


class BQDataSearchAgent(BaseAgent):
    
    def __init__(self, search_engine: BQSearchTable):
        super().__init__(
            name="BQDataSearcher",
            capabilities=[
                "table_search",
                "metadata_retrieval",
                "similarity_matching",
                "result_ranking"
            ]
        )
        self.search_engine = search_engine
    

    async def process_task(self, task: AgentTask) -> AgentTask:
        self.status = AgentStatus.WORKING
        
        try:
            search_params = task.input_data
            query = search_params.get("query", "")
            limit = search_params.get("limit", 10)
            filters = search_params.get("filters", {})
            
            results = self.search_engine.search(query, limit)
            if filters:
                results = self._apply_filters(results, filters)
            
            enhanced_results = self._enhance_results(results)
            
            task.output_data = {
                "query": query,
                "results": enhanced_results,
                "total_found": len(enhanced_results),
                "search_metadata": {
                    "search_time": datetime.now(),
                    "filters_applied": filters,
                    "enhancement_applied": True
                }
            }
            
            task.status = AgentStatus.COMPLETED
            self.logger.info(f"BQ Search completed: {len(enhanced_results)} results found")
            
        except Exception as e:
            task.status = AgentStatus.FAILED
            task.output_data = {"error": str(e)}
            self.logger.error(f"BQ Search failed: {e}")
        
        self.status = AgentStatus.IDLE
        return task

    
    def _apply_filters(self, results: List[Dict], filters: Dict) -> List[Dict]:
        filtered_results = results.copy()
        
        if "dataset" in filters:
            target_datasets = filters["dataset"]
            if isinstance(target_datasets, str):
                target_datasets = [target_datasets]
            filtered_results = [r for r in filtered_results 
                             if any(ds in r["table_name"] for ds in target_datasets)]
        
        if "min_score" in filters:
            min_score = filters["min_score"]
            filtered_results = [r for r in filtered_results 
                             if r["relevance_score"] >= min_score]
        
        if "tags" in filters:
            required_tags = filters["tags"]
            if isinstance(required_tags, str):
                required_tags = [required_tags]
            filtered_results = [r for r in filtered_results 
                             if any(tag in r["tags"] for tag in required_tags)]
        
        return filtered_results

    
    def _enhance_results(self, results: List[Dict]) -> List[Dict]:
        enhanced = []
        
        for result in results:
            enhanced_result = result.copy()
            enhanced_result["usage_recommendation"] = self._get_usage_recommendation(result)
            enhanced_result["data_freshness"] = self._calculate_data_freshness(result)
            enhanced_result["related_tables"] = self._find_related_tables(result)
            
            enhanced.append(enhanced_result)
        
        return enhanced
    
    def _get_usage_recommendation(self, result: Dict) -> str:
        tags = result.get("tags", [])
        row_count = result.get("row_count", 0)
        
        if "daily" in tags and row_count > 10000:
            return "High-volume daily data - suitable for trend analysis"
        elif "campaign" in tags:
            return "Marketing campaign data - good for performance analysis"
        elif row_count < 1000:
            return "Small reference table - suitable for lookup operations"
        else:
            return "General purpose table - verify data recency before use"

    
    def _calculate_data_freshness(self, result: Dict) -> str:
        last_modified = result.get("last_modified", "")
        
        if not last_modified:
            return "Unknown"
        
        try:
            from datetime import datetime, timedelta
            
            mod_date = datetime.strptime(last_modified, "%Y-%m-%d")
            days_old = (datetime.now() - mod_date).days
            
            if days_old <= 1:
                return "Very Fresh (< 1 day)"
            elif days_old <= 7:
                return "Fresh (< 1 week)"
            elif days_old <= 30:
                return "Recent (< 1 month)"
            else:
                return f"Older ({days_old} days)"
                
        except:
            return "Unknown"

    
    def _find_related_tables(self, result: Dict) -> List[str]:
        current_tags = set(result.get("tags", []))
        current_dataset = result.get("dataset", "")
        
        related = []
        for table in self.search_engine.tables:
            if table.get_full_name() == result["table_name"]:
                continue
                
            table_tags = set(table.tags)
            common_tags = current_tags.intersection(table_tags)
            
            if len(common_tags) >= 2 or table.dataset == current_dataset:
                related.append(table.get_full_name())
                
            if len(related) >= 3: 
                break
        
        return related