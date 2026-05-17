import asyncio
import time

from src import log
from src.pipeline import CollectorPipeline


async def main() -> None:
    log.info("Starting collector")

    while True:
        try:
            pipeline = CollectorPipeline()
            await pipeline.run()
        except Exception as exc:
            log.exception("Collector pipeline failed", error=str(exc))
        finally:
            time.sleep(24 * 3600.0)  # Sleep for 24 hours before running the pipeline again


if __name__ == "__main__":
    asyncio.run(main())
