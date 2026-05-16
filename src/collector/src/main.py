from __future__ import annotations

import os
from pathlib import Path


def main() -> None:
    sources_config = Path(os.getenv("SOURCES_CONFIG", "/app/configs/sources.yaml"))
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    kafka_topic = os.getenv("KAFKA_TOPIC_RAW_VACANCIES", "joblake.raw.vacancies")

    if not sources_config.exists():
        raise FileNotFoundError(f"Sources config does not exist: {sources_config}")

    text = sources_config.read_text(encoding="utf-8")
    if "sources:" not in text:
        raise ValueError(f"Sources config has no sources section: {sources_config}")

    enabled_sources = [
        line.strip().removeprefix("- name:").strip()
        for line in text.splitlines()
        if line.strip().startswith("- name:")
    ]

    print("JobLake collector preflight")
    print(f"sources_config={sources_config}")
    print(f"configured_sources={enabled_sources}")
    print(f"kafka_bootstrap_servers={kafka_bootstrap_servers}")
    print(f"kafka_topic={kafka_topic}")
    print("status=ok")


if __name__ == "__main__":
    main()
