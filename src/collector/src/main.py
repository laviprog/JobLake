import asyncio

from src import log
from src.pipeline import CollectorPipeline


async def main() -> None:
    log.info("Starting collector")

    pipeline = CollectorPipeline()
    await pipeline.run()


if __name__ == "__main__":
    asyncio.run(main())
