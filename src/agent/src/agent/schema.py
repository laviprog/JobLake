from pydantic import Field

from src.schema import BaseSchema


class ChatMessage(BaseSchema):
    role: str = Field(..., examples=["user", "assistant"])
    content: str


class ChatRequest(BaseSchema):
    message: str = Field(..., min_length=1)
    history: list[ChatMessage] = Field(default_factory=list)


class ChatResponse(BaseSchema):
    answer: str
