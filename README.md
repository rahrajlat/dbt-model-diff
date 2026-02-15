# dbt-model-diff

```{=html}
<p align="center">
  <img src="https://img.shields.io/badge/python-3.9%2B-blue.svg" />
  <img src="https://img.shields.io/badge/dbt-compatible-orange.svg" />
  <img src="https://img.shields.io/badge/license-MIT-green.svg" />
  <img src="https://img.shields.io/badge/status-v1.0.0-blue" />
</p>
```
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

-   Git worktree isolation (no macro overrides required)
-   Data-level comparison between branches
-   Key-based row diff
-   Schema difference detection (added/removed columns)
-   Column profiling (% nulls, uniqueness ratio)
-   JSON output for CI pipelines
-   Markdown output for PR comments
-   Rich CLI output for local debugging
-   Docker-based integration test
-   Postgres + Redshift support (v1)

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

## 🏗 Architecture Overview

                    ┌──────────────────────┐
                    │   Git Worktrees      │
                    │ (base / head refs)   │
                    └─────────┬────────────┘
                              │
                    ┌─────────▼────────────┐
                    │   dbt build (x2)     │
                    └─────────┬────────────┘
                              │
                    ┌─────────▼────────────┐
                    │ Manifest Resolution  │
                    └─────────┬────────────┘
                              │
                    ┌─────────▼────────────┐
                    │ Snapshot to Diff     │
                    │ Schema (CTAS)        │
                    └─────────┬────────────┘
                              │
                    ┌─────────▼────────────┐
                    │ Data Comparison      │
                    │ Row + Schema + Stats │
                    └─────────┬────────────┘
                              │
                    ┌─────────▼────────────┐
                    │ Output Formatter     │
                    │ (rich/json/md)       │
                    └──────────────────────┘

Modular design enables easy extension to additional warehouses.

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

## 🎥 Demo (CLI Walkthrough)

You can record a short demo using:

``` bash
asciinema rec demo.cast
dbt-model-diff diff dim_customers --keys customer_id ...
exit
```

Then embed in README:

``` html
<script src="https://asciinema.org/a/your-demo-id.js" async></script>
```

A short 30-second demo significantly improves discoverability and
credibility.

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

## 🧪 Testing

Run all tests:

``` bash
pytest
```

Run integration only:

``` bash
make e2e-test
```

------------------------------------------------------------------------

## 📦 Publishing to PyPI

Build distribution:

``` bash
python -m build
```

Upload (after configuring API token):

``` bash
twine upload dist/*
```

Tag release:

``` bash
git tag v1.0.0
git push origin v1.0.0
```

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
