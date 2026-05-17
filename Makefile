.PHONY: compose-config build-infra up down logs ps collector

compose-config:
	docker compose config --quiet

build-infra:
	docker compose build spark-master spark-worker agent app collector

up:
	docker compose up --build

down:
	docker compose down

logs:
	docker compose logs -f

ps:
	docker compose ps
