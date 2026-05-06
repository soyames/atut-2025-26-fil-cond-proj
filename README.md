# Industrialized ETL Pipeline (Fil conducteur)

This project implements an end-to-end ETL pipeline with three data sources, transformation logic, orchestration, and object storage.

## What this project does

The pipeline runs in three stages:

1. **Extract**
   - Web source: `https://books.toscrape.com/` (catalog + product detail pages)
   - CSV source: `Books.csv`
   - SQL source: local SQLite database initialized from script
2. **Transform**
   - Cleans and normalizes fields
   - Handles Windows Spark runtime constraints with a fallback path
   - Produces curated outputs with a unified schema
3. **Load**
   - Uploads curated outputs to MinIO (`lakehouse/curated/...`)


## Local quick start

From project root:

```powershell
Copy-Item .env.example .env
uv sync --dev
uv run python scripts\init_sql_source.py
.\scripts\run_pipeline_local.ps1
```

You can also run each stage independently:

```powershell
uv run python -m src.etl.jobs.run_extract
uv run python -m src.etl.jobs.run_transform
uv run python -m src.etl.jobs.run_load
```

## Run the full platform with Docker

```powershell
docker compose up -d
```

Services:
- Airflow: `http://localhost:8091` (`admin/admin`)
- MinIO: `http://localhost:9001` (`minioadmin/minioadmin`)
- Spark master UI: `http://localhost:8081`

## Extracted web fields

The web extractor reads listing pages and product detail pages.  
Main fields include:

`title, category, upc, product_type, price_gbp, price_excl_tax_gbp, price_incl_tax_gbp, tax_gbp, rating, availability, availability_count, num_reviews, description, product_page_url, source_page, ingestion_date`

## Curated outputs

Bucket path in MinIO:

`lakehouse/curated`

Datasets:
- `books_curated`
- `web_catalog_curated`

Both curated datasets use the same schema and column order:

`record_source,title,isbn,author,year,publisher,publisher_country,language,category,upc,product_type,price_gbp,price_excl_tax_gbp,price_incl_tax_gbp,tax_gbp,rating,availability,availability_count,num_reviews,description,product_page_url,source_page,image_url_s,image_url_m,image_url_l,ingestion_date`

## Tests

```powershell
uv run pytest
```
