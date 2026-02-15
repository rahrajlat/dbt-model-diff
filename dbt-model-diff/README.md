# dbt-model-diff

Compare **dbt model output data** between a base git ref (e.g. `main`) and a head git ref (e.g. a feature branch).

## Warehouses

- ✅ Postgres
- ✅ Redshift (implemented)
- 🚧 Snowflake (next)

## Install (dev)

```bash
pip install -e .
```

## Usage

Rich (default):

```bash
dbt-model-diff diff dim_customers --keys customer_id --base main --head feature/Change-dims --profiles-dir .
```

JSON:

```bash
dbt-model-diff diff dim_customers --keys customer_id --format json > diff.json
```

Markdown:

```bash
dbt-model-diff diff dim_customers --keys customer_id --format markdown
```

## Notes for Redshift

- Uses `SVV_COLUMNS` to list columns (recommended in Redshift docs).
- Uses `md5()` for row hashing (supported in Redshift).
