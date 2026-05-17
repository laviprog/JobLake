from src import log
from src.config import settings
from src.habr_career_parser import HabrCareerParser
from src.publisher import VacancyPublisher


class CollectorPipeline:
    def __init__(self) -> None:
        self._habr_parser = HabrCareerParser()

    async def run(self) -> None:
        max_pages = settings.HABR_CAREER_MAX_PAGES

        log.info("Collector pipeline started", max_pages=max_pages)

        links = self._habr_parser.collect_all_links(max_pages=max_pages)

        parsed_count = 0
        published_count = 0
        failed_count = 0

        async with VacancyPublisher() as publisher:
            for index, link in enumerate(links, start=1):
                log.info(
                    "Processing vacancy",
                    index=index,
                    total=len(links),
                    url=link,
                )

                try:
                    vacancy = self._habr_parser.parse_vacancy(link)
                    parsed_count += 1
                except Exception as exc:
                    failed_count += 1
                    log.exception("Failed to parse vacancy", url=link, error=str(exc))

                    await publisher.publish_error(
                        {
                            "stage": "parse",
                            "source": "habr_career",
                            "url": link,
                            "error": str(exc),
                        }
                    )
                    continue

                try:
                    await publisher.publish_vacancy(vacancy)
                    published_count += 1
                except Exception as exc:
                    failed_count += 1
                    log.exception(
                        "Failed to publish vacancy",
                        vacancy_id=vacancy.id,
                        url=vacancy.url,
                        error=str(exc),
                    )

                    await publisher.publish_error(
                        {
                            "stage": "publish",
                            "source": vacancy.source,
                            "vacancy_id": vacancy.id,
                            "url": vacancy.url,
                            "error": str(exc),
                        }
                    )

        log.info(
            "Collector pipeline finished",
            links_count=len(links),
            parsed_count=parsed_count,
            published_count=published_count,
            failed_count=failed_count,
        )
