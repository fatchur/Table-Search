

import logging
from typing import Dict, List
from datetime import datetime

from pkg.big_query.services.table_search import BQSearchTable
from pkg.agentic.service.orchestrator_agent import BQAgentOrchestrator


class BQAgenticDataCatalogueInterface:
    
    def __init__(self, search_engine: BQSearchTable):
        self.orchestrator = BQAgentOrchestrator(search_engine)
        self.session_history = []
        self.logger = logging.getLogger("BQInterface")
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    async def chat(self, user_query: str, user_id: str = "default") -> str:

        self.session_history.append({
            "timestamp": datetime.now(),
            "user_query": user_query,
            "user_id": user_id
        })
        
        response = await self.orchestrator.process_query(user_query, user_id)
        self.session_history[-1]["response"] = response
        
        return response

    
    def get_agent_diagnostics(self) -> Dict:
        return {
            "agent_status": self.orchestrator.get_agent_status(),
            "session_queries": len(self.session_history),
            "system_uptime": datetime.now().isoformat()
        }

    
    def get_session_history(self) -> List[Dict]:
        return self.session_history.copy()

    
    async def run_cli(self):
        print("\n" + "="*70)
        print("Agentic AI BigQuery Data Catalogue - Intelligent Data Discovery")
        print("="*70)
        print("Ask me anything about your BigQuery data catalogue!")
        print("Examples:")
        print("  • 'Find tables with daily campaign data'")
        print("  • 'What's the schema of marketing.campaign_performance?'")
        print("  • 'Show me metadata for sales tables'")
        print("  • 'Which tables contain user behavior data?'")
        print("\nType 'quit' to exit, 'help' for more commands")
        print("="*70)
        
        while True:
            try:
                user_input = input("\n Ask me: ").strip()
                
                if not user_input:
                    continue
                
                if user_input.lower() in ['quit', 'exit', 'q']:
                    print("\n Thank you for using Agentic AI BigQuery Data Catalogue!")
                    await self.orchestrator.shutdown()
                    break
                
                elif user_input.lower() == 'help':
                    self._show_help()
                    continue
                
                elif user_input.lower() == 'status':
                    self._show_agent_status()
                    continue
                
                elif user_input.lower() == 'history':
                    self._show_session_history()
                    continue
                
                print("\n Processing your BigQuery request...")
                response = await self.chat(user_input)
                print(f"\n AI Response:\n{response}")
                
            except KeyboardInterrupt:
                print("\n\n Goodbye!")
                await self.orchestrator.shutdown()
                break
            except Exception as e:
                print(f"\n Error: {e}")

    
    def _show_help(self):
        """Show help information"""
        print("\n Available Commands:")
        print("─" * 50)
        print(" BigQuery Data Discovery:")
        print("  • 'Find tables about [topic]'")
        print("  • 'Search for [keyword] data'")
        print("  • 'Tables in [dataset] dataset'")
        print("\n  System Commands:")
        print("  • 'status' - Show agent status")
        print("  • 'history' - Show session history")
        print("  • 'help' - Show this help")
        print("  • 'quit' - Exit the system")

    
    def _show_agent_status(self):
        status = self.get_agent_diagnostics()
        
        print("\n BigQuery Agent Status:")
        print("─" * 30)
        for agent_name, agent_status in status["agent_status"].items():
            print(f" {agent_name}: {agent_status}")
        
        print(f"\n Session Stats:")
        print(f"  • Queries processed: {status['session_queries']}")
        print(f"  • System uptime: {status['system_uptime']}")

    
    def _show_session_history(self):
        if not self.session_history:
            print("\n No queries in current session")
            return
        
        print(f"\n Session History ({len(self.session_history)} queries):")
        print("─" * 50)
        
        for i, entry in enumerate(self.session_history[-5:], 1):  # Show last 5
            timestamp = entry["timestamp"].strftime("%H:%M:%S")
            query = entry["user_query"][:50] + "..." if len(entry["user_query"]) > 50 else entry["user_query"]
            print(f"{i}. [{timestamp}] {query}")