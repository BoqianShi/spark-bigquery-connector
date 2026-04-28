# Module Inventory

Authoritative source: root [pom.xml](../../../pom.xml) for module activation per profile, and [spark-bigquery-parent/pom.xml](../../../spark-bigquery-parent/pom.xml) for shared config + the `integration` / `acceptance` profiles.

Modules listed under `<modules>` directly (not behind a profile) are **always built**. Everything else activates only when its profile is selected.

## Always built (no profile required)

| Path | Purpose |
|---|---|
| [spark-bigquery-parent/](../../../spark-bigquery-parent/) | Parent POM — versions, plugin config, dependency mgmt, defines `integration` and `acceptance` profiles. |
| [bigquery-connector-common/](../../../bigquery-connector-common/) | Pure BigQuery API utilities. No Spark dependency. Reusable by non-Spark callers. |
| [spark-bigquery-tests/](../../../spark-bigquery-tests/) | Shared integration-test fixtures and helpers. |
| [spark-bigquery-scala-212-support/](../../../spark-bigquery-scala-212-support/) | Scala 2.12 compatibility shims. |
| [spark-bigquery-connector-common/](../../../spark-bigquery-connector-common/) | Shared Spark-aware logic across DSv1/DSv2. **Prefer adding new shared code here.** |
| [spark-bigquery-python-lib/](../../../spark-bigquery-python-lib/) | Python helpers for BigQuery types not native to Spark. |

## DSv1 (Scala) — `-Pdsv1_<scala>` or aggregate `-Pdsv1`

| Path | Active in profile(s) | Purpose |
|---|---|---|
| [spark-bigquery-dsv1/spark-bigquery-dsv1-parent/](../../../spark-bigquery-dsv1/spark-bigquery-dsv1-parent/) | all dsv1_* | DSv1 sub-parent POM. |
| [spark-bigquery-dsv1/spark-bigquery-dsv1-spark2-support/](../../../spark-bigquery-dsv1/spark-bigquery-dsv1-spark2-support/) | all dsv1_* | Spark 2.x API shim. |
| [spark-bigquery-dsv1/spark-bigquery-dsv1-spark3-support/](../../../spark-bigquery-dsv1/spark-bigquery-dsv1-spark3-support/) | all dsv1_* | Spark 3.x API shim (depends on Spark 3 itself). |
| [spark-bigquery-dsv1/spark-bigquery_2.11/](../../../spark-bigquery-dsv1/spark-bigquery_2.11/) | dsv1_2.11 | Scala 2.11 connector — legacy stub (only a `pom.xml`, no source), not in CI. |
| [spark-bigquery-dsv1/spark-bigquery_2.12/](../../../spark-bigquery-dsv1/spark-bigquery_2.12/) | dsv1_2.12 | Scala 2.12 connector. Source is near-duplicated with the 2.13 module; only `src/build` is symlinked to the shared `spark-bigquery-dsv1/src/build/`. |
| [spark-bigquery-dsv1/spark-bigquery_2.13/](../../../spark-bigquery-dsv1/spark-bigquery_2.13/) | dsv1_2.13 | Scala 2.13 connector. Source is near-duplicated with the 2.12 module; only `src/build` is symlinked to the shared `spark-bigquery-dsv1/src/build/`. |
| [spark-bigquery-dsv1/spark-bigquery-with-dependencies-parent/](../../../spark-bigquery-dsv1/spark-bigquery-with-dependencies-parent/) | all dsv1_* | Shared config for the shaded jars. |
| [spark-bigquery-dsv1/spark-bigquery-with-dependencies_2.11/](../../../spark-bigquery-dsv1/spark-bigquery-with-dependencies_2.11/) | dsv1_2.11 | Shaded fat-jar — Scala 2.11. Legacy. |
| [spark-bigquery-dsv1/spark-bigquery-with-dependencies_2.12/](../../../spark-bigquery-dsv1/spark-bigquery-with-dependencies_2.12/) | dsv1_2.12 | Shaded fat-jar — Scala 2.12. **Primary DSv1 distribution artifact.** |
| [spark-bigquery-dsv1/spark-bigquery-with-dependencies_2.13/](../../../spark-bigquery-dsv1/spark-bigquery-with-dependencies_2.13/) | dsv1_2.13 | Shaded fat-jar — Scala 2.13. |

## DSv2 (Java) — `-Pdsv2_<spark>` or aggregate `-Pdsv2`

The DSv2 build uses a **ladder**: each `*-bigquery-lib` module is reused by the next-Spark-version connector. So building `dsv2_4.1` pulls in every `*-bigquery-lib` from 3.1 through 4.1 (see the root POM `<profiles>` block to confirm which libs each profile includes).

| Path | Active in profile(s) | Purpose |
|---|---|---|
| [spark-bigquery-dsv2/spark-bigquery-dsv2-parent/](../../../spark-bigquery-dsv2/spark-bigquery-dsv2-parent/) | all dsv2_* | DSv2 sub-parent POM. |
| [spark-bigquery-dsv2/spark-bigquery-dsv2-common/](../../../spark-bigquery-dsv2/spark-bigquery-dsv2-common/) | all dsv2_* | Shared DSv2 logic. |
| [spark-bigquery-dsv2/spark-bigquery-metrics/](../../../spark-bigquery-dsv2/spark-bigquery-metrics/) | dsv2_3.2+ | Metrics module. |
| [spark-bigquery-dsv2/spark-2.4-bigquery/](../../../spark-bigquery-dsv2/spark-2.4-bigquery/) | dsv2_2.4 | Spark 2.4 connector — legacy, not in CI. |
| [spark-bigquery-dsv2/spark-3.1-bigquery-lib/](../../../spark-bigquery-dsv2/spark-3.1-bigquery-lib/) | dsv2_3.1+ | Foundation lib reused by all newer connectors. |
| [spark-bigquery-dsv2/spark-3.1-bigquery/](../../../spark-bigquery-dsv2/spark-3.1-bigquery/) | dsv2_3.1 | Spark 3.1 final connector jar. |
| [spark-bigquery-dsv2/spark-3.2-bigquery-lib/](../../../spark-bigquery-dsv2/spark-3.2-bigquery-lib/) | dsv2_3.2+ | |
| [spark-bigquery-dsv2/spark-3.2-bigquery/](../../../spark-bigquery-dsv2/spark-3.2-bigquery/) | dsv2_3.2 | Spark 3.2 final connector jar. |
| [spark-bigquery-dsv2/spark-3.3-bigquery-lib/](../../../spark-bigquery-dsv2/spark-3.3-bigquery-lib/) | dsv2_3.3+ | |
| [spark-bigquery-dsv2/spark-3.3-bigquery/](../../../spark-bigquery-dsv2/spark-3.3-bigquery/) | dsv2_3.3 | Spark 3.3 final connector jar. |
| [spark-bigquery-dsv2/spark-3.4-bigquery-lib/](../../../spark-bigquery-dsv2/spark-3.4-bigquery-lib/) | dsv2_3.4+ | |
| [spark-bigquery-dsv2/spark-3.4-bigquery/](../../../spark-bigquery-dsv2/spark-3.4-bigquery/) | dsv2_3.4 | Spark 3.4 final connector jar. |
| [spark-bigquery-dsv2/spark-3.5-bigquery-lib/](../../../spark-bigquery-dsv2/spark-3.5-bigquery-lib/) | dsv2_3.5+ | |
| [spark-bigquery-dsv2/spark-3.5-bigquery/](../../../spark-bigquery-dsv2/spark-3.5-bigquery/) | dsv2_3.5 | Spark 3.5 final connector jar. |
| [spark-bigquery-dsv2/spark-4.0-bigquery-lib/](../../../spark-bigquery-dsv2/spark-4.0-bigquery-lib/) | dsv2_4.0+ | |
| [spark-bigquery-dsv2/spark-4.0-bigquery/](../../../spark-bigquery-dsv2/spark-4.0-bigquery/) | dsv2_4.0 | Spark 4.0 final connector jar. |
| [spark-bigquery-dsv2/spark-4.1-bigquery-lib/](../../../spark-bigquery-dsv2/spark-4.1-bigquery-lib/) | dsv2_4.1 | |
| [spark-bigquery-dsv2/spark-4.1-bigquery/](../../../spark-bigquery-dsv2/spark-4.1-bigquery/) | dsv2_4.1 | Spark 4.1 final connector jar. |

## Pushdown — `spark-bigquery-pushdown/`

A separate sub-tree with its own internal profile setup. **Not driven by the root `dsv1*` / `dsv2*` profiles**, and currently not exercised by `cloudbuild/presubmit.sh`. Build it directly under its own POM if needed.

| Path | Purpose |
|---|---|
| [spark-bigquery-pushdown/spark-bigquery-pushdown-parent/](../../../spark-bigquery-pushdown/spark-bigquery-pushdown-parent/) | Sub-parent POM. |
| [spark-bigquery-pushdown/pushdown_common_src/](../../../spark-bigquery-pushdown/pushdown_common_src/) | Shared pushdown source tree. |
| [spark-bigquery-pushdown/spark-bigquery-pushdown-common_2.{11,12,13}/](../../../spark-bigquery-pushdown/) | Common pushdown jars per Scala version. |
| spark-{2.4,3.1,3.2,3.3}-bigquery-pushdown_{2.11,2.12,2.13} | Per Spark+Scala pushdown variants (see directory listing under [spark-bigquery-pushdown/](../../../spark-bigquery-pushdown/)). |

## Coverage — `-Pcoverage`

| Path | Purpose |
|---|---|
| [coverage/](../../../coverage/) | Jacoco aggregator — `jacoco:report-aggregate` rolls up per-module coverage here. |
