
from abc import ABC, abstractmethod
from typing import Any, List
import logging
import asyncio

from domains.values.agent_status import AgentStatus
from domains.models.agent_task import AgentTask
from domains.models.agent_message import AgentMessage


class BaseAgent(ABC):
    
    def __init__(self, name: str, capabilities: List[str]):
        self.name = name
        self.capabilities = capabilities
        self.status = AgentStatus.IDLE
        self.message_queue = asyncio.Queue()
        self.task_history: List[AgentTask] = []
        self.logger = logging.getLogger(f"Agent.{name}")

    
    @abstractmethod
    async def process_task(self, task: AgentTask) -> AgentTask:
        pass
    

    async def send_message(self, recipient: str, message_type: str, content: Any, orchestrator):
        message = AgentMessage(
            sender=self.name,
            recipient=recipient,
            message_type=message_type,
            content=content
        )
        await orchestrator.route_message(message)
    

    def can_handle(self, task_type: str) -> bool:
        return task_type in self.capabilities