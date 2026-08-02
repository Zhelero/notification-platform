from pydantic import BaseModel


class NotifyRequest(BaseModel):
    user_id: int
    channel: str
    text: str
