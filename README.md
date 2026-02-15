<p align="center">
  <img src="assets/banner.png" alt="dbt-model-diff banner" width="100%" />
</p>

# dbt-model-diff

> Compare **dbt model outputs at the data level** across Git branches.

`dbt-model-diff` is an open-source CLI tool that compares the actual data produced by a dbt model between two Git refs (e.g. `main` vs `feature/*`) — not just SQL differences.

It answers the most important question in analytics engineering:

> **Did my change alter the data?**

---

## 🚀 Why This Tool Exists

Most dbt review workflows compare:

- SQL changes  
- Compiled artifacts  
- Manifest diffs  

But those approaches do **not guarantee that the resulting data remains consistent**.

`dbt-model-diff` builds both versions of a model and performs a deterministic data comparison.

This enables:

- Safer refactors
- CI data validation
- Regression detection
- Data contract enforcement
- Migration confidence

---

## ✨ Key Features

- Git worktree isolation (no macro overrides required)
- Data-level comparison between branches
- Key-based row diff
- Schema difference detection (added/removed columns)
- Column profiling (% nulls, uniqueness ratio)
- JSON output for CI pipelines
- Markdown output for PR comments
- Rich CLI output for local debugging
- Docker-based integration test
- Postgres + Redshift support (v1)

---

## 🧠 How It Works

1. Creates Git worktrees for `base` and `head`
2. Runs `dbt build` independently in each worktree
3. Reads `manifest.json` to locate built relations
4. Copies relations into an isolated diff schema
5. Compares:
   - Row counts
   - Key-based differences
   - Column statistics
   - Schema changes
6. Outputs results in `rich`, `json`, or `markdown`

No changes to your dbt project are required.

---

## 🧪 Example Usage

```bash
dbt-model-diff diff dim_customers \
  --keys customer_id \
  --base main \
  --head feature/include-4 \
  --profiles-dir . \
  --project-dir . \
  --format rich