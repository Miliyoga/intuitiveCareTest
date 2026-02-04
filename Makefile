.PHONY: run pipeline

run:
	uvicorn app.main:app --reload --port 8000

pipeline:
	python -c "from app.jobs.full_pipeline_job import run_full_pipeline; run_full_pipeline()"

up:
	docker compose build --no-cache loader
	docker compose up --build

down:
	docker compose down -v