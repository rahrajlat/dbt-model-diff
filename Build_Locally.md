
# 🛠 Build & Run Locally (Postgres + Demo dbt Project)

This guide explains how to run **dbt-model-diff** locally using:

- Docker-based Postgres
- A demo dbt project
- Editable Python installation

---

## ✅ Prerequisites

- Docker (Docker Desktop on macOS/Linux)
- Python 3.9+
- Git
- `dbt-postgres`
- A working `profiles.yml`

---

# 1️⃣ Start Postgres with Docker

If your repo includes a Docker setup for Postgres, use the Makefile.

### Build Postgres image (optional)

```bash
make postgres-build
```

### Start Postgres

```bash
make postgres-up
```

### Verify container is running

```bash
make postgres-ps
```

### View logs (optional)

```bash
make postgres-logs
```

---

# 2️⃣ Install dbt-model-diff (Editable Mode)

From the repo root:

```bash
python -m venv .venv_dbt_ddiff
source .venv_dbt_ddiff/bin/activate   # macOS / Linux
```

Install the package:

```bash
pip install -e .
```

Install dbt + test dependencies:

```bash
pip install dbt-postgres pytest
```

---

# 3️⃣ Use the Demo dbt Project

Navigate to your demo project:

```bash
cd dbt/dbt_warehouse_demo_with_docs
```

---

## Configure profiles.yml

Example local Postgres profile:

```yaml
demo_project:
  target: dev
  outputs:
    dev:
      type: postgres
      host: 127.0.0.1
      user: postgres
      password: postgres
      port: 5432
      dbname: postgres
      schema: core
```

---

## Validate dbt Setup

```bash
dbt debug --profiles-dir .
```

Build models (sanity check):

```bash
dbt build --profiles-dir .
```

---

# 4️⃣ Run dbt-model-diff

From repo root (or use full paths):

```bash
dbt-model-diff diff dim_customers   --keys customer_id   --base main   --head feature/include-4   --profiles-dir dbt/dbt_warehouse_demo_with_docs   --project-dir dbt/dbt_warehouse_demo_with_docs   --format rich
```

---

## JSON Output (CI Mode)

```bash
dbt-model-diff diff dim_customers   --keys customer_id   --base main   --head feature/include-4   --profiles-dir dbt/dbt_warehouse_demo_with_docs   --project-dir dbt/dbt_warehouse_demo_with_docs   --format json
```

---

## Markdown Output (PR Comments)

```bash
dbt-model-diff diff dim_customers   --keys customer_id   --base main   --head feature/include-4   --profiles-dir dbt/dbt_warehouse_demo_with_docs   --project-dir dbt/dbt_warehouse_demo_with_docs   --format markdown
```

---

# 5️⃣ Run Docker-based E2E Test

This test:

- Spins up Postgres
- Creates temporary Git branches
- Builds dbt models
- Validates diff output

Run:

```bash
make e2e-test
```

Debug mode:

```bash
make e2e-test-debug
```

---

# 6️⃣ Cleanup

Stop Postgres:

```bash
make postgres-down
```

Remove containers + volumes:

```bash
make postgres-clean
```

Full reset (also removes image):

```bash
make postgres-reset
```

---

# 🔎 Notes

- If `--keys` is omitted, only row counts, schema diff, and column statistics are computed.
- Ensure `--base` and `--head` Git refs exist locally.
- Update Postgres credentials if using a custom configuration.
