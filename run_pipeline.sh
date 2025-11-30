#!/usr/bin/env bash
set -euo pipefail

# Ask user input
read -p "Mau scraping mobil bekas apa?: " KEYWORD
read -p "Slug (nama file pendek, misal 'supra'): " SLUG

# logs file
LOG_FILE="logs/${SLUG}_$(date +%Y%m%d_%H%M%S).log"

# Start logging after input
exec > >(tee "$LOG_FILE") 2>&1

echo "== Run pipeline for '$KEYWORD' ($SLUG) =="
echo "Log saved at: $LOG_FILE"

# --Mapping file path
HTML_PATH="data/raw_html/${SLUG}.html"
PARSED_PATH="data/parsed/${SLUG}.csv"
TRANSFORMED_PATH="data/transformed/${SLUG}_transformed.csv"
INSERTED_PATH="data/inserted/${SLUG}_inserted.json"

# activated virtual environment
source .venv/bin/activate  #optional

# 3) Export env dari .env
export $(grep -v '^#' .env | xargs)

# 4) Naikkan Postgres
#docker compose up -d db_mentoring_w8

# 5) Jalankan Luigi
python scraps.py Load \
  --local-scheduler \
  --keyword "$KEYWORD" \
  --html-path "$HTML_PATH" \
  --parsed-path "$PARSED_PATH" \
  --transformed-path "$TRANSFORMED_PATH" \
  --inserted-path "$INSERTED_PATH" \
  2>&1 | tee "logs/${SLUG}_$(date +%Y%m%d_%H%M%S).log"

