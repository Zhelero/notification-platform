from pydantic import BaseModel
from uuid import UUID

class NotifyRequest(BaseModel):
    user_id: int
    channel: str
    text: str
    correlation_id: UUID | None = None
