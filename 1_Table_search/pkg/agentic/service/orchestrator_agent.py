
import uuid
import asyncio
import logging
from typing import Dict, List

from domains.values.agent_status import AgentStatus
from domains.models.agent_task import AgentTask
from domains.models.agent_message import AgentMessage
from domains.services.base_agent import BaseAgent
from pkg.agentic.service.query_analysis_agent import QueryAnalysisAgent
from pkg.agentic.service.data_search_agent import BQDataSearchAgent
from pkg.agentic.service.metadata_agent import BQMetadataAgent
from pkg.agentic.service.response_agent import ResponseGenerationAgent
from pkg.big_query.services.table_search import BQSearchTable


class BQAgentOrchestrator:
    
    def __init__(self, search_engine: BQSearchTable):
        self.agents: Dict[str, BaseAgent] = {}
        self.task_queue = asyncio.Queue()
        self.active_workflows: Dict[str, Dict] = {}
        self.logger = logging.getLogger("BQOrchestrator")
        
        self._initialize_agents(search_engine)

    
    def _initialize_agents(self, search_engine: BQSearchTable):
        self.agents["QueryAnalyzer"] = QueryAnalysisAgent()
        self.agents["BQDataSearcher"] = BQDataSearchAgent(search_engine)
        self.agents["BQMetadataManager"] = BQMetadataAgent()
        self.agents["ResponseGenerator"] = ResponseGenerationAgent()
        
        self.logger.info("All BigQuery agents initialized")

    
    async def process_query(self, user_query: str, user_id: str = "default") -> str:
        workflow_id = str(uuid.uuid4())
        
        try:
            self.logger.info(f"Processing BigQuery query: '{user_query}' (Workflow: {workflow_id})")
            
            analysis_task = AgentTask(
                task_type="query_understanding",
                input_data={"query": user_query}
            )
            
            analyzed_task = await self.agents["QueryAnalyzer"].process_task(analysis_task)
            
            if analyzed_task.status == AgentStatus.FAILED:
                return "I'm sorry, I couldn't understand your BigQuery query. Could you please rephrase it?"
            
            query_analysis = analyzed_task.output_data
            intent = query_analysis["intent"]
            
            search_task = AgentTask(
                task_type="table_search",
                input_data={
                    "query": user_query,
                    "limit": 10,
                    "filters": self._generate_filters(query_analysis)
                }
            )
            
            search_result = await self.agents["BQDataSearcher"].process_task(search_task)
            
            if search_result.status == AgentStatus.FAILED:
                return "I encountered an error while searching BigQuery data. Please try again."
            
            search_data = search_result.output_data
            response_task = AgentTask(
                task_type="natural_language_generation",
                input_data={
                    "original_query": user_query,
                    "search_results": search_data["results"],
                    "intent": intent,
                    "query_analysis": query_analysis
                }
            )
            
            response_result = await self.agents["ResponseGenerator"].process_task(response_task)
            
            if response_result.status == AgentStatus.FAILED:
                return "I found some BigQuery results but had trouble formatting the response. Here's what I found: " + str(search_data["results"][:2])
            
            self.logger.info(f"BigQuery workflow {workflow_id} completed successfully")
            return response_result.output_data["response"]
            
        except Exception as e:
            self.logger.error(f"BigQuery workflow {workflow_id} failed: {e}")
            return "I'm experiencing technical difficulties with BigQuery search. Please try again later."
    
    
    def _generate_filters(self, query_analysis: Dict) -> Dict:
        filters = {}
        
        entities = query_analysis.get("entities", {})
        if entities.get("datasets"):
            filters["dataset"] = entities["datasets"]
        
        confidence = query_analysis.get("confidence", 0.7)
        if confidence > 0.8:
            filters["min_score"] = 0.1
        else:
            filters["min_score"] = 0.05
        
        return filters

    
    async def route_message(self, message: AgentMessage):
        if message.recipient in self.agents:
            await self.agents[message.recipient].receive_message(message)
        else:
            self.logger.warning(f"Unknown recipient: {message.recipient}")

    
    def get_agent_status(self) -> Dict[str, str]:
        return {name: agent.status.value for name, agent in self.agents.items()}

    
    async def shutdown(self):
        self.logger.info("Shutting down BigQuery orchestrator and all agents")