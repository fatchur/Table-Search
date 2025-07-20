
from typing import Any
from dataclasses import dataclass, field
from datetime import datetime
import uuid


@dataclass
class AgentMessage:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    sender: str = ""
    recipient: str = ""
    message_type: str = ""
    content: Any = None
    timestamp: datetime = field(default_factory=datetime.now)
    priority: int = 1