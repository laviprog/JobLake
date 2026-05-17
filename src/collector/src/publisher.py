from typing import Any

from faststream.kafka import KafkaBroker

from src import log
from src.config import settings
from src.schema import Vacancy


class VacancyPublisher:
    def __init__(self) -> None:
        self._broker = KafkaBroker(
            settings.KAFKA_BOOTSTRAP_SERVERS,
            acks="all",
            linger_ms=10,
            compression_type="gzip",
        )
        self._vacancy_pub = self._broker.publisher(settings.KAFKA_TOPIC_RAW)
        self._error_pub = self._broker.publisher(settings.KAFKA_TOPIC_ERROR)

    async def __aenter__(self) -> "VacancyPublisher":
        await self._broker.start()
        log.info("Kafka broker connected", bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
        return self

    async def __aexit__(self, *args: object) -> None:
        await self._broker.stop()
        log.info("Kafka broker disconnected")

    async def publish_vacancy(self, vacancy: Vacancy) -> None:
        log.debug(
            "Publishing vacancy",
            vacancy_id=vacancy.id,
            title=vacancy.title,
            company=vacancy.company.name,
            topic=settings.KAFKA_TOPIC_RAW,
        )

        await self._vacancy_pub.publish(
            message=vacancy.model_dump(),
            key=vacancy.id.encode("utf-8"),
            headers={
                "source": vacancy.source,
                "schema_version": "1",
                "content_type": "application/json",
            },
        )

        log.info(
            "Published vacancy",
            vacancy_id=vacancy.id,
            topic=settings.KAFKA_TOPIC_RAW,
        )

    async def publish_error(self, error_data: dict[str, Any]) -> None:
        await self._error_pub.publish(
            message=error_data,
            key=error_data["id"].encode("utf-8") if "id" in error_data else None,
            headers={
                "source": error_data["source"],
                "schema_version": "1",
                "content_type": "application/json",
            },
        )

        log.warning("Error published to DLT", error_data=error_data)
