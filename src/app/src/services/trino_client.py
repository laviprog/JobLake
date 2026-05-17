from typing import Any

import trino.dbapi
import trino.exceptions

from src.config import settings


class TrinoQueryError(RuntimeError):
    """Raised when Trino cannot execute or return a query result."""


class TrinoClient:
    """
    Small wrapper around the official Trino DB-API client.
    """

    def fetch_all(
        self,
        sql: str,
        params: tuple[Any, ...] | None = None,
    ) -> list[dict[str, Any]]:
        conn = trino.dbapi.connect(
            host=settings.TRINO_HOST,
            port=settings.TRINO_PORT,
            user=settings.TRINO_USER,
            catalog=settings.TRINO_CATALOG,
            schema=settings.TRINO_SCHEMA,
            http_scheme=settings.TRINO_SCHEME,
            source="joblake-dashboard",
            request_timeout=settings.TRINO_TIMEOUT_SECONDS,
        )

        try:
            cursor = conn.cursor()
            cursor.execute(sql, params or ())

            columns = [column[0] for column in cursor.description or []]
            rows = cursor.fetchall()

            return [dict(zip(columns, row, strict=False)) for row in rows]
        except trino.exceptions.TrinoQueryError as exc:
            raise TrinoQueryError(exc.message) from exc
        except trino.exceptions.Error as exc:
            raise TrinoQueryError(str(exc)) from exc
        except TimeoutError as exc:
            raise TrinoQueryError("Trino request timed out.") from exc
        finally:
            conn.close()
