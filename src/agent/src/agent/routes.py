from fastapi import APIRouter

from src.agent.agent import agent
from src.agent.schema import ChatRequest, ChatResponse

router = APIRouter(prefix="/agent", tags=["Agent"])


@router.post("/chat")
async def chat(payload: ChatRequest) -> ChatResponse:
    """
    Handle chat messages from the client and generate responses using the agent.
    """
    answer = await agent.chat(
        message=payload.message,
        history=payload.history,
    )
    return ChatResponse(answer=answer)
