import inspect
import json
import time
from typing import Any

import structlog
from ollama import AsyncClient

from src import log
from src.agent.prompts import SYSTEM_PROMPT
from src.agent.schema import ChatMessage
from src.agent.tools import AVAILABLE_TOOLS, TOOLS
from src.config import settings
from src.utils import generate_uuid


class JobLakeAgent:
    def __init__(self) -> None:
        self._client = AsyncClient(host=settings.OLLAMA_BASE_URL)

    async def chat(self, message: str, history: list[ChatMessage] | None = None) -> str:
        chat_id = generate_uuid()
        started_at = time.perf_counter()

        with structlog.contextvars.bound_contextvars(chat_id=chat_id):
            return await self._chat(message=message, history=history, started_at=started_at)

    async def _chat(
        self,
        message: str,
        history: list[ChatMessage] | None,
        started_at: float,
    ) -> str:
        history_count = len(history or [])

        log.info(
            "Agent chat started",
            model=settings.OLLAMA_MODEL_NAME,
            history_messages=history_count,
            message_length=len(message),
        )

        messages: list[dict[str, Any]] = [
            {
                "role": "system",
                "content": SYSTEM_PROMPT.strip(),
            }
        ]

        for item in history or []:
            if item.role in {"user", "assistant"}:
                messages.append(
                    {
                        "role": item.role,
                        "content": item.content,
                    }
                )

        messages.append(
            {
                "role": "user",
                "content": message,
            }
        )

        max_tool_rounds = 5

        for round_number in range(1, max_tool_rounds + 1):
            round_started_at = time.perf_counter()
            log.debug(
                "Ollama chat request started",
                round_number=round_number,
                messages_count=len(messages),
                tools_count=len(TOOLS),
            )

            try:
                response = await self._client.chat(
                    model=settings.OLLAMA_MODEL_NAME,
                    messages=messages,
                    tools=TOOLS,
                    options={
                        "temperature": 0.1,
                    },
                )
            except Exception:
                log.exception(
                    "Ollama chat request failed",
                    round_number=round_number,
                    duration_ms=self._duration_ms(round_started_at),
                )
                raise

            assistant_message = response.message
            messages.append(assistant_message)

            tool_calls = assistant_message.tool_calls or []
            log.info(
                "Ollama chat response received",
                round_number=round_number,
                duration_ms=self._duration_ms(round_started_at),
                tool_calls_count=len(tool_calls),
                answer_length=len(assistant_message.content or ""),
            )

            if not tool_calls:
                answer = assistant_message.content or ""
                log.info(
                    "Agent chat completed",
                    rounds=round_number,
                    duration_ms=self._duration_ms(started_at),
                    answer_length=len(answer),
                )
                return answer

            for tool_call in tool_calls:
                tool_name = tool_call.function.name
                tool_args = tool_call.function.arguments or {}

                tool_started_at = time.perf_counter()
                log.info(
                    "Agent tool call started",
                    tool_name=tool_name,
                    argument_keys=sorted(tool_args),
                )

                tool = AVAILABLE_TOOLS.get(tool_name)

                if tool is None:
                    log.warning("Agent tool not found", tool_name=tool_name)
                    tool_result: Any = {
                        "error": f"Unknown tool: {tool_name}",
                    }
                else:
                    try:
                        result = tool(**tool_args)
                        tool_result = await result if inspect.isawaitable(result) else result
                    except Exception as exc:  # noqa: BLE001
                        log.exception(
                            "Agent tool call failed",
                            tool_name=tool_name,
                            duration_ms=self._duration_ms(tool_started_at),
                        )
                        tool_result = {
                            "error": str(exc),
                            "tool": tool_name,
                        }

                if not isinstance(tool_result, str):
                    tool_result = json.dumps(tool_result, ensure_ascii=False, default=str)

                log.info(
                    "Agent tool call completed",
                    tool_name=tool_name,
                    duration_ms=self._duration_ms(tool_started_at),
                    result_length=len(tool_result),
                )

                messages.append(
                    {
                        "role": "tool",
                        "tool_name": tool_name,
                        "content": tool_result,
                    }
                )

        log.warning(
            "Agent chat stopped after max tool rounds",
            max_tool_rounds=max_tool_rounds,
            duration_ms=self._duration_ms(started_at),
        )
        return (
            "Я сделал несколько обращений к данным, но не смог завершить ответ за допустимое "
            "число tool-вызовов. Попробуйте сузить вопрос: период, навык, город, компания "
            "или источник."
        )

    @staticmethod
    def _duration_ms(started_at: float) -> float:
        return round((time.perf_counter() - started_at) * 1000, 2)


agent = JobLakeAgent()
