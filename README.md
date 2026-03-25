# Shufersal Price Monitor - Tempo

Hebrew RTL dashboard for monitoring Shufersal product prices and promotions in real-time.

## Features

- Live price tracking across all Shufersal store formats
- Promo detection and unit-price normalization
- Price trend charts per format
- Alert system (red/yellow/green severity)
- Scheduled data pipeline (3x daily)

## Tech Stack

- **Backend:** FastAPI + aiosqlite (SQLite)
- **Frontend:** Vanilla JS + Tailwind CSS + Chart.js (no build step)
- **Scheduler:** APScheduler (Asia/Jerusalem timezone)
- **Containerization:** Docker

## Local Development

### Requirements
- Python 3.12+

### Setup

```bash
# Install dependencies
pip install -r backend/requirements.txt

# Run the server
python -m uvicorn backend.main:app --reload --port 8000
```

Open http://localhost:8000

## Docker

```bash
docker-compose up
```

## Deployment (Fly.io)

```bash
fly launch
fly volumes create prices_data --size 3
fly deploy
```

## Environment Variables

Copy `.env.example` to `.env` and fill in values (if any are required for your setup).

## Data

The SQLite database (`data/prices.db`) is excluded from version control.
Data is populated automatically by the scraper pipeline on startup and on schedule.
