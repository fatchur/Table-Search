
from typing import Any, Dict, Optional
from dataclasses import dataclass, field
from datetime import datetime
import uuid

from domains.values.agent_status import AgentStatus


@dataclass
class AgentTask:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    task_type: str = ""
    input_data: Any = None
    output_data: Any = None
    status: AgentStatus = AgentStatus.IDLE
    assigned_agent: str = ""
    created_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    metadata: Dict = field(default_factory=dict)