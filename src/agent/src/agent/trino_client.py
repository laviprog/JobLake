import time
from typing import Any

import aiotrino

from src import log
from src.config import settings


class TrinoClient:
    """
    Async Trino wrapper.
    """

    async def fetch_all(
        self,
        sql: str,
        params: tuple[Any, ...] | None = None,
    ) -> list[dict[str, Any]]:
        started_at = time.perf_counter()
        query_params = params or ()

        log.debug(
            "Trino query started",
            host=settings.TRINO_HOST,
            port=settings.TRINO_PORT,
            catalog=settings.TRINO_CATALOG,
            schema=settings.TRINO_SCHEMA,
            params_count=len(query_params),
            sql_preview=" ".join(sql.split())[:200],
        )

        try:
            async with aiotrino.dbapi.connect(
                host=settings.TRINO_HOST,
                port=settings.TRINO_PORT,
                user=settings.TRINO_USER,
                catalog=settings.TRINO_CATALOG,
                schema=settings.TRINO_SCHEMA,
                http_scheme=settings.TRINO_SCHEME,
            ) as conn:
                cur = await conn.cursor()
                await cur.execute(sql, query_params)
                rows = await cur.fetchall()

                columns = [column[0] for column in await cur.get_description() or []]
                result = [dict(zip(columns, row, strict=False)) for row in rows]

                log.info(
                    "Trino query completed",
                    rows_count=len(result),
                    columns_count=len(columns),
                    duration_ms=self._duration_ms(started_at),
                )
                return result
        except Exception:
            log.exception(
                "Trino query failed",
                duration_ms=self._duration_ms(started_at),
                params_count=len(query_params),
            )
            raise

    @staticmethod
    def _duration_ms(started_at: float) -> float:
        return round((time.perf_counter() - started_at) * 1000, 2)


trino_client = TrinoClient()
