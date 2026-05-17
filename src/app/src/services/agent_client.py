import json
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from src.config import settings


class AgentClientError(RuntimeError):
    """Raised when the dashboard cannot communicate with the AI agent API."""


class AgentClient:
    def __init__(self) -> None:
        self._chat_url = f"{settings.AGENT_URL.rstrip('/')}/agent/chat"

    def chat(self, message: str, history: Sequence[Mapping[str, str]]) -> str:
        payload = {
            "message": message,
            "history": [
                {"role": item["role"], "content": item["content"]}
                for item in history
                if item.get("role") in {"user", "assistant"} and item.get("content")
            ],
        }
        request = Request(
            self._chat_url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urlopen(request, timeout=settings.AGENT_TIMEOUT_SECONDS) as response:
                data: dict[str, Any] = json.load(response)
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise AgentClientError(f"Agent HTTP {exc.code}: {detail}") from exc
        except URLError as exc:
            raise AgentClientError(f"Agent is unavailable: {exc.reason}") from exc
        except TimeoutError as exc:
            raise AgentClientError("Agent request timed out.") from exc
        except json.JSONDecodeError as exc:
            raise AgentClientError("Agent returned an invalid JSON response.") from exc

        answer = data.get("answer")
        if not isinstance(answer, str):
            raise AgentClientError("Agent response does not contain an answer.")
        return answer
