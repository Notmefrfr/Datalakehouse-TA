# Data Lakehouse Analytics Platform

A prototype Data Lakehouse platform built using Apache Spark, MinIO, PostgreSQL, and Tableau to simplify data ingestion, cleaning, management, and analytics.

## Features

- Upload standardized datasets
- Incremental ETL pipeline
- Bronze → Silver → Gold architecture
- Automatic dataset merging
- Manual data cleaning
- Duplicate handling options
- Dataset management
- Metadata tracking
- Role-based access (Admin & Employee)
- PostgreSQL integration
- Tableau-ready output
- Interactive web dashboard

## Tech Stack

- Python
- Apache Spark
- Flask (Web Application)
- PostgreSQL
- MinIO
- Tableau
- Docker

## Architecture

```
Upload
   ↓
Bronze (Raw)
   ↓
Silver (Clean)
   ↓
Gold (Analytics)
   ↓
PostgreSQL
   ↓
Tableau
```

## Roadmap

- [x] Incremental ETL
- [x] Bronze/Silver/Gold pipeline
- [x] Dashboard Prototype
- [ ] Flask Web UI
- [ ] Login & Role Management
- [ ] Automatic Dataset Merge
- [ ] Metadata Management
- [ ] PostgreSQL Integration
- [ ] Tableau Integration
- [ ] Production Deployment

## Status

🚧 Currently under active development.

This project is being developed as a production-oriented Data Lakehouse platform for standardized enterprise data management.
