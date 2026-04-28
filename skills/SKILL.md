---
name: spark-bigquery-connector
description: Repo-orientation skill for the Apache Spark connector for Google BigQuery. Use when working in this repo on any Maven module — choosing a build profile for a Spark+Scala combo, running unit / integration / acceptance tests, debugging shading or version-shim issues across DSv1 / DSv2 / pushdown, or wiring up GCP env vars for tests. Defers to cloudbuild/presubmit.sh and cloudbuild/nightly.sh as the source of truth for test commands.
---

# spark-bigquery-connector

Internal/dev-facing orientation for working in this repo. For *user-facing* connector usage (Spark options, read/write APIs, compatibility matrix) defer to the top-level [README.md](../../../README.md).

Two companion files in this directory go deeper:
- [modules.md](modules.md) — full module inventory.
- [testing.md](testing.md) — test recipes mapped to the cloudbuild scripts.

## What this connector is

A Spark SQL DataSource for Google BigQuery built on the BigQuery Storage Read/Write API (Arrow & Avro over gRPC). It implements column projection, predicate pushdown, dynamic sharding for parallel reads, and several write modes (direct via Storage Write API, indirect via GCS staging). It is shipped as multiple jars — one per Spark + Scala + API-generation combination.

## Mental model: three axes

Almost everything about the build / test / artifact layout falls out of three axes:

1. **API generation**
   - **DSv1** — Scala-based, legacy Spark DataSource V1 API. Source is in `spark-bigquery-dsv1/`. Distributed as a shaded "with-dependencies" jar.
   - **DSv2** — Java-based, modern Spark DataSource V2 API. Source is in `spark-bigquery-dsv2/`. One jar per supported Spark minor version.

2. **Spark / Scala matrix**
   - DSv1 → Scala **2.12** and **2.13** (a Scala 2.11 module still exists on disk but is no longer in the CI matrix).
   - DSv2 → Spark **3.1, 3.2, 3.3, 3.4, 3.5, 4.0, 4.1** (a Spark 2.4 module still exists on disk but is no longer in the CI matrix).

3. **Pushdown** — `spark-bigquery-pushdown/` adds query pushdown variants per Spark/Scala combo, layered on top of the connector. This module tree has its own internal profiles and is not driven by the root `dsv1*` / `dsv2*` profiles.

## Module map (high level)

See [modules.md](modules.md) for the complete table. Five things to know:

- **`spark-bigquery-parent/`** — parent POM. Versions, plugin config, `integration` / `acceptance` profiles live here. (`pom.xml` of this dir.)
- **`bigquery-connector-common/`** — pure BigQuery utilities, no Spark dependency.
- **`spark-bigquery-connector-common/`** — Spark-aware shared logic across DSv1 and DSv2. **Prefer landing new shared code here.**
- **`spark-bigquery-dsv1/`** — DSv1 connectors. The Scala 2.12 and 2.13 modules keep **near-duplicate `src/main` and `src/test` trees** (intentional — only the per-Scala-version provider differs, e.g. `Scala212BigQueryRelationProvider` vs `Scala213BigQueryRelationProvider`). **Shared build configuration only** is symlinked: each module's `src/build` → `spark-bigquery-dsv1/src/build/`. Changes that aren't Scala-version-specific need to be applied to both modules.
- **`spark-bigquery-dsv2/`** — per-Spark-version DSv2 modules plus `spark-bigquery-dsv2-common`, `spark-bigquery-metrics`, and `*-bigquery-lib` modules used as building blocks for the next-version connector.

Also: `spark-bigquery-pushdown/`, `spark-bigquery-tests/` (shared test fixtures), `spark-bigquery-scala-212-support/`, `spark-bigquery-python-lib/`, `coverage/` (Jacoco aggregator), `examples/`, `scripts/`.

## Profile cheatsheet

Profiles defined in root [pom.xml](../../../pom.xml):

| Profile | What it builds |
|---|---|
| *(none)* | Only the always-on common modules: parent, `bigquery-connector-common`, `spark-bigquery-tests`, `spark-bigquery-scala-212-support`, `spark-bigquery-connector-common`, `spark-bigquery-python-lib`. |
| `dsv1_2.12`, `dsv1_2.13` | One Scala variant of the DSv1 connector. |
| `dsv1_2.11` | Legacy — still in the POM, not in the CI matrix. |
| `dsv2_3.1` … `dsv2_3.5`, `dsv2_4.0`, `dsv2_4.1` | One Spark version of the DSv2 connector (each profile pulls all `*-bigquery-lib` predecessors it needs). |
| `dsv2_2.4` | Legacy — still in the POM, not in the CI matrix. |
| `dsv1`, `dsv2`, `all` | Aggregate profiles spanning both Scala variants / all Spark variants. |
| `coverage` | Adds the `coverage/` aggregator for Jacoco reports. |

Plus profiles defined in [spark-bigquery-parent/pom.xml](../../../spark-bigquery-parent/pom.xml):

| Profile | What it does |
|---|---|
| `integration` | Enables `*IntegrationTest` classes via the Failsafe plugin. |
| `acceptance` | Enables `*AcceptanceTest` classes via the Failsafe plugin (forks ~10 in parallel). |

Profiles compose with commas: `-Pcoverage,integration,dsv2_3.5`.

## How to build and test (quick)

The CI scripts in `cloudbuild/` are the source of truth — see [testing.md](testing.md) for the exact recipes. Three most common local commands:

```bash
# Build a single connector flavor (no tests)
./mvnw install -DskipTests -Pdsv2_3.5

# Run unit tests for one flavor
./mvnw test -Pcoverage,dsv2_3.5

# Run integration tests for one flavor (needs GCP env vars — see below)
./mvnw failsafe:integration-test failsafe:verify -Pcoverage,integration,dsv2_3.5
```

To match a full presubmit locally, see [testing.md](testing.md).

## JDK requirements (gotcha)

The CI uses **two JDKs in the same pipeline**:

- **Java 17** — initial install, all unit tests, and the integration test stages for Spark 3.3 / 3.4 / 3.5 / 4.0 / 4.1.
- **Java 8** — integration test stages for `dsv1_2.12`, `dsv1_2.13`, `dsv2_3.1`, `dsv2_3.2`.

CI builds `toolchains.xml` automatically via `./mvnw toolchains:generate-jdk-toolchains-xml`. Locally, install both JDKs and either generate `toolchains.xml` the same way or set `JAVA_HOME` per command. Source of truth for which stage uses which: [cloudbuild/presubmit.sh](../../../cloudbuild/presubmit.sh).

## GCP env vars for integration / acceptance tests

Required:
- `GOOGLE_APPLICATION_CREDENTIALS` — path to a service-account JSON or a `gcloud auth login` credential.
- `GOOGLE_CLOUD_PROJECT` — test project ID.
- `TEMPORARY_GCS_BUCKET` — write-staging bucket for integration tests.
- `BIGLAKE_CONNECTION_ID` — Cloud Resource connection used by BigLake table tests.
- `BIGQUERY_KMS_KEY_NAME` — KMS key for encrypted-table tests.

Acceptance-only:
- `ACCEPTANCE_TEST_BUCKET`
- `SERVERLESS_NETWORK_URI` — VPC network for serverless Dataproc batches.

The test-runner GCP user needs permissions to create/delete BQ datasets+tables in the test project, and to read/write/delete objects in the test buckets.

## Code-org rules contributors hit

- **DSv1 has near-duplicate `src/` trees** between `spark-bigquery_2.12` and `spark-bigquery_2.13` — apply non-Scala-version-specific changes to both. Only `src/build` is symlinked to a shared `spark-bigquery-dsv1/src/build/`.
- **New shared logic → `spark-bigquery-connector-common`**, not a per-version module, unless the API surface genuinely differs across Spark versions.
- **Shaded "with-dependencies" jars** are produced by the `spark-bigquery-with-dependencies_2.1{2,3}` modules and are how DSv1 ships externally.
- **DSv2 ladders via `*-bigquery-lib`** — e.g., `spark-3.5-bigquery-lib` is consumed by the `spark-3.5-bigquery` final jar and is also pulled in (alongside earlier libs) when building any newer Spark version. Inspect each `dsv2_*` profile in the root POM to see the chain.
- **Scala style** is enforced via [scalastyle-config.xml](../../../scalastyle-config.xml).
- **Java baseline is 8** for compiled output (`maven.compiler.release=8` in the parent POM); the build can run on a newer JDK but the bytecode target stays 8.
- **`${revision}`** drives the version (default `0.0.1-SNAPSHOT`); nightly CI overrides it via `-Drevision=…`.

## Where to look for X

- **"How does CI run tests?"** → [cloudbuild/presubmit.sh](../../../cloudbuild/presubmit.sh), [cloudbuild/nightly.sh](../../../cloudbuild/nightly.sh). **These are source of truth — do not infer commands from CONTRIBUTING.md alone.**
- **"What versions are supported in a given release?"** → compatibility matrix in [README.md](../../../README.md).
- **"What changed?"** → [CHANGES.md](../../../CHANGES.md).
- **"How are modules wired?"** → [pom.xml](../../../pom.xml) (root, defines profiles + module activation) and each module's own `pom.xml`.
- **"Where do tests live?"** → standard Maven layout: `src/test/java` and `src/test/scala` per module, plus shared fixtures in [spark-bigquery-tests/](../../../spark-bigquery-tests/).

## When NOT to use this skill

- For *using* the connector from a Spark job (read/write options, performance tuning, Dataproc setup) — go to [README.md](../../../README.md).
- For deciding which version of the connector to depend on — go to the README compatibility matrix.
- For Spark itself or BigQuery API semantics — those are upstream concerns, not in this repo.
