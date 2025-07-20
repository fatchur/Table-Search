

from typing import Dict, List
from datetime import datetime

from domains.values.agent_status import AgentStatus
from domains.models.agent_task import AgentTask
from domains.services.base_agent import BaseAgent



class ResponseGenerationAgent(BaseAgent):
    
    def __init__(self):
        super().__init__(
            name="ResponseGenerator",
            capabilities=[
                "natural_language_generation",
                "response_formatting",
                "context_integration",
                "explanation_generation"
            ]
        )
    
    async def process_task(self, task: AgentTask) -> AgentTask:
        self.status = AgentStatus.WORKING
        
        try:
            input_data = task.input_data
            original_query = input_data.get("original_query", "")
            search_results = input_data.get("search_results", [])
            intent = input_data.get("intent", "general_search")
            
            if intent == "table_search":
                response = self._generate_table_search_response(original_query, search_results)
            else:
                response = self._generate_general_response(original_query, search_results)
            
            task.output_data = {
                "response": response,
                "response_type": intent,
                "generated_at": datetime.now().isoformat()
            }
            
            task.status = AgentStatus.COMPLETED
            
        except Exception as e:
            task.status = AgentStatus.FAILED
            task.output_data = {"error": str(e)}
            self.logger.error(f"Response generation failed: {e}")
        
        self.status = AgentStatus.IDLE
        return task

    
    def _generate_table_search_response(self, query: str, results: List[Dict]) -> str:
        if not results:
            return f"I couldn't find any BigQuery tables matching '{query}'. Try using different keywords or check if the table exists in your data warehouse."
        
        response = f"I found {len(results)} BigQuery table(s) related to '{query}':\n\n"
        
        for i, result in enumerate(results[:5], 1):  # Show top 5
            response += f"{i}. **{result['table_name']}**\n"
            response += f"   - {result['description']}\n"
            response += f"   - Relevance: {result['relevance_score']}\n"
            response += f"   - Tags: {', '.join(result['tags'])}\n"
            
            if 'usage_recommendation' in result:
                response += f"   - 💡 {result['usage_recommendation']}\n"
            
            response += "\n"
        
        if len(results) > 5:
            response += f"... and {len(results) - 5} more results.\n"
        
        return response

    
    def _generate_general_response(self, query: str, results: List[Dict]) -> str:
        if not results:
            return f"I couldn't find specific BigQuery information for '{query}'. Could you try rephrasing your question or being more specific about what you're looking for?"
        
        response = f"Based on your query '{query}', here's what I found in BigQuery:\n\n"
        
        for i, result in enumerate(results[:10], 1):  # Show top 10
            response += f"{i}. **{result['table_name']}**\n"
            response += f"   {result['description']}\n\n"
        
        return response