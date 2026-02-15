# dbt-model-diff

> Compare **dbt model outputs at the data level** across Git branches.

`dbt-model-diff` is an open-source CLI tool that compares the actual
data produced by a dbt model between two Git refs (e.g. `main` vs
`feature/*`) --- not just SQL differences.

It answers the most important question in analytics engineering:

> **"Did my change alter the data?"**

------------------------------------------------------------------------

## 🚀 Why dbt-model-diff?

Traditional review workflows compare:

-   SQL changes
-   Compiled dbt artifacts
-   Manifest diffs

But none of those guarantee that the **resulting data** remains
consistent.

`dbt-model-diff` executes both versions of a model, snapshots their
results, and performs a deterministic comparison.

This enables:

-   Safer refactors
-   CI data validation
-   Regression detection
-   Data contract enforcement
-   Migration confidence

------------------------------------------------------------------------

## ✨ Key Features

-   ✅ Git worktree isolation (no macro overrides required)
-   ✅ Data-level comparison between branches
-   ✅ Key-based row diff
-   ✅ Schema difference detection (added/removed columns)
-   ✅ Column profiling (% nulls, uniqueness ratio)
-   ✅ JSON output for CI pipelines
-   ✅ Markdown output for PR comments
-   ✅ Rich CLI output for local debugging
-   ✅ Docker-based integration test
-   ✅ Postgres + Redshift support (v1)

------------------------------------------------------------------------

## 🧠 How It Works

1.  Creates Git worktrees for `base` and `head` refs\
2.  Runs `dbt build` independently in each worktree\
3.  Reads `manifest.json` to locate built relations\
4.  Copies relations into an isolated diff schema\
5.  Compares:
    -   Row counts
    -   Key-based row differences
    -   Column statistics
    -   Schema differences\
6.  Outputs results in the selected format (`rich`, `json`, `markdown`)

No changes to your dbt project are required.

------------------------------------------------------------------------

## 📦 Installation

``` bash
pip install -e .
```

Development setup:

``` bash
pip install -e .
pip install pytest dbt-postgres
```

------------------------------------------------------------------------

## 🧪 Example Usage

``` bash
dbt-model-diff diff dim_customers   --keys customer_id   --base main   --head feature/include-4   --profiles-dir .   --project-dir .   --format rich
```

------------------------------------------------------------------------

## 📊 Example Output

### Rich Output

    Base rowcount:   3
    Head rowcount:   4
    Added rows:      1
    Removed rows:    0
    Changed rows:    0

### JSON Output (CI-friendly)

``` json
{
  "rowcounts": { "base": 3, "head": 4 },
  "row_diff": {
    "added": 1,
    "removed": 0,
    "changed": 0
  }
}
```

------------------------------------------------------------------------

## 🐳 End-to-End Integration Test

The project includes a full Docker-based integration test that:

-   Spins up Postgres
-   Creates a temporary Git repo
-   Builds dbt models in two branches
-   Verifies diff correctness

Run:

``` bash
make e2e-test
```

------------------------------------------------------------------------

## 🏗 Architecture

The project is structured with clear separation of concerns:

-   `cli/` -- Command-line interface
-   `core/` -- Diff orchestration logic
-   `adapters/` -- Warehouse-specific implementations
-   `formatters/` -- Output rendering
-   `tests/` -- Unit and integration tests

The adapter abstraction layer enables future support for additional
warehouses (e.g., Snowflake).

------------------------------------------------------------------------

## 📌 Roadmap

-   Snowflake support
-   CI fail-on-diff flag
-   Automatic primary key detection from dbt tests
-   GitHub Action template
-   Performance optimizations for large tables

------------------------------------------------------------------------

## 🤝 Contributing

Contributions are welcome!

If you'd like to: - Add support for new warehouses - Improve diff
performance - Extend output formats - Improve testing coverage

Open an issue or submit a PR.

------------------------------------------------------------------------

## 📄 License

MIT License

------------------------------------------------------------------------

## 👤 Author

Rahul Rajasekharan\
Senior Data Engineer & Open Source Contributor

Building tooling for modern data platforms.
