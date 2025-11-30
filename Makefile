# Makefile for OLX ETL Pipeline
# Common commands:
#   make help
#   make venv
#   make install
#   make docker-up
#   make run
#   make clean

PYTHON      ?= python3
VENV_DIR    ?= .venv
PYTHON_BIN  := $(VENV_DIR)/bin/python
PIP_BIN     := $(VENV_DIR)/bin/pip
DC          ?= docker compose

# Default keyword/slug for ad-hoc runs (can be overridden: make run KEYWORD="..." SLUG="...")
KEYWORD     ?= "Toyota Calya"
SLUG        ?= calya

.PHONY: help venv install playwright docker-up docker-down run scrape lint format test clean clean-data clean-logs

help:
	@echo "Available targets:"
	@echo "  make venv         - Create virtual environment in $(VENV_DIR)"
	@echo "  make install      - Install Python dependencies from requirements.txt"
	@echo "  make playwright   - Install Playwright browsers"
	@echo "  make docker-up    - Start PostgreSQL via docker-compose"
	@echo "  make docker-down  - Stop all docker-compose services"
	@echo "  make run          - Run full Luigi pipeline via run_pipeline.sh"
	@echo "  make scrape       - Run single Luigi Load task with KEYWORD/SLUG"
	@echo "  make lint         - Run ruff linting (if installed)"
	@echo "  make format       - Run black formatting (if installed)"
	@echo "  make test         - Run pytest (if tests are present)"
	@echo "  make clean        - Remove caches, __pycache__, and build artifacts"
	@echo "  make clean-data   - Remove generated data/*.csv, *.json, raw_html, transformed"
	@echo "  make clean-logs   - Remove logs/*"

venv:
	$(PYTHON) -m venv $(VENV_DIR)
	@echo "Virtual environment created at $(VENV_DIR)"

install: venv
	$(PIP_BIN) install --upgrade pip
	$(PIP_BIN) install -r requirements.txt
	@echo "Dependencies installed."

playwright:
	$(PYTHON_BIN) -m playwright install
	@echo "Playwright browsers installed."

docker-up:
	$(DC) up -d
	@echo "docker-compose services started."

docker-down:
	$(DC) down
	@echo "docker-compose services stopped."

# Run full pipeline using your helper script
run:
	bash run_pipeline.sh

# Run a single Luigi Load task manually (can override KEYWORD/SLUG)
scrape:
	$(PYTHON_BIN) scraps.py Load \
	  --local-scheduler \
	  --keyword "$(KEYWORD)" \
	  --html-path "data/raw_html/$(SLUG).html" \
	  --parsed-path "data/parsed/$(SLUG).csv" \
	  --transformed-path "data/transformed/$(SLUG)_transformed.csv" \
	  --inserted-path "data/inserted/$(SLUG)_inserted.json"

lint:
	@if command -v ruff >/dev/null 2>&1; then \
	  echo "Running ruff..."; \
	  ruff check .; \
	else \
	  echo "ruff not installed. Install with 'pip install ruff'."; \
	fi

format:
	@if command -v black >/dev/null 2>&1; then \
	  echo "Running black..."; \
	  black .; \
	else \
	  echo "black not installed. Install with 'pip install black'."; \
	fi

test:
	@if command -v pytest >/dev/null 2>&1; then \
	  echo "Running pytest..."; \
	  pytest; \
	else \
	  echo "pytest not installed. Install with 'pip install pytest'."; \
	fi

clean:
	find . -name "__pycache__" -type d -exec rm -rf {} +
	find . -name "*.pyc" -delete
	find . -name "*.pyo" -delete
	rm -rf build/ dist/ *.egg-info
	@echo "Cleaned Python caches and build artifacts."

clean-data:
	rm -rf data/raw_html/*
	rm -rf data/parsed/*
	rm -rf data/transformed/*
	rm -rf data/inserted/*
	@echo "Cleaned generated data files."

clean-logs:
	rm -rf logs/*
	@echo "Cleaned log files."

clean-all:
	rm -rf data/raw_html/*
	rm -rf data/parsed/*
	rm -rf data/transformed/*
	rm -rf data/inserted/*
	@echo "Cleaned generated data files."

	rm -rf logs/*
	@echo "Cleaned log files."

	rm -rf screenshots/*
	@echo "Cleaned screenshots files"
