# Testing Cheatsheet

Source of truth for what CI runs:
- [cloudbuild/presubmit.sh](../../../cloudbuild/presubmit.sh) — presubmit (init, unit tests, integration tests per Spark/Scala).
- [cloudbuild/nightly.sh](../../../cloudbuild/nightly.sh) — nightly (everything above + acceptance tests).
- [cloudbuild/cloudbuild.yaml](../../../cloudbuild/cloudbuild.yaml) — wiring + which env vars are passed to each step.

If a recipe in this file ever drifts from those scripts, the scripts win.

---

## Test types

| Type | Plugin | Filename pattern | Activated by |
|---|---|---|---|
| Unit | Surefire | `*Test.java` (default) | always (within selected profile) |
| Integration | Failsafe | `*IntegrationTest.java` | `-Pintegration` |
| Acceptance | Failsafe | `*AcceptanceTest.java` | `-Pacceptance` (forks ~10 in parallel) |

---

## CI build options

CI sets these flags (cf. `presubmit.sh` lines ~28–30):

```bash
BUILD_OPTS='-Xss1g -Xmx20g -XX:MaxMetaspaceSize=10g -XX:ReservedCodeCacheSize=2g -Dsun.zip.disableMemoryMapping=true -DtrimStackTrace=false'
MVN="./mvnw -B -e -s cloudbuild/gcp-settings.xml -Dmaven.repo.local=.repository -t toolchains.xml"
```

If you hit OOMs locally, replicate `BUILD_OPTS` via `MAVEN_OPTS`:
```bash
export MAVEN_OPTS='-Xss1g -Xmx20g -XX:MaxMetaspaceSize=10g -XX:ReservedCodeCacheSize=2g'
```

---

## Presubmit — exact CI sequence

The CI runs these in order. To simulate a presubmit locally, follow the same order; you can drop profiles you don't need.

### Step `init` — install everything (no tests)

```bash
export JAVA_HOME=$JAVA17_HOME
./mvnw -T 1C -t toolchains.xml install -DskipTests \
  -Pdsv1_2.12,dsv1_2.13,dsv2_3.1,dsv2_3.2,dsv2_3.3,dsv2_3.4,dsv2_3.5,dsv2_4.0,dsv2_4.1
```

### Step `unittest` — unit tests + coverage

```bash
export JAVA_HOME=$JAVA17_HOME
./mvnw -T 1C test jacoco:report jacoco:report-aggregate \
  -Pcoverage,dsv1_2.12,dsv1_2.13,dsv2_3.1,dsv2_3.2,dsv2_3.3,dsv2_3.4,dsv2_3.5,dsv2_4.0,dsv2_4.1
```

### Steps `integrationtest-<spark>` — integration tests, sharded by profile

CI runs nine of these in parallel/dependent stages. Per-stage JDK:

| Step | Profile | JDK |
|---|---|---|
| `integrationtest-2.12` | `dsv1_2.12` | **Java 8** |
| `integrationtest-2.13` | `dsv1_2.13` | **Java 8** |
| `integrationtest-3.1`  | `dsv2_3.1`  | **Java 8** |
| `integrationtest-3.2`  | `dsv2_3.2`  | **Java 8** |
| `integrationtest-3.3`  | `dsv2_3.3`  | **Java 17** |
| `integrationtest-3.4`  | `dsv2_3.4`  | **Java 17** |
| `integrationtest-3.5`  | `dsv2_3.5`  | **Java 17** |
| `integrationtest-4.0`  | `dsv2_4.0`  | **Java 17** |
| `integrationtest-4.1`  | `dsv2_4.1`  | **Java 17** |

Each step is:

```bash
export JAVA_HOME=$JAVA8_HOME   # or $JAVA17_HOME per the table above
./mvnw -t toolchains.xml failsafe:integration-test failsafe:verify \
  jacoco:report jacoco:report-aggregate \
  -Pcoverage,integration,<profile>
```

---

## Nightly — exact CI sequence

Adds acceptance tests to the presubmit sequence. From `nightly.sh`:

```bash
# Build
export JAVA_HOME=$JAVA17_HOME
./mvnw -T 1C install -DskipTests -Pdsv1_2.12,dsv1_2.13,dsv2_3.1,dsv2_3.2,dsv2_3.3,dsv2_3.4,dsv2_3.5,dsv2_4.0,dsv2_4.1

# Unit tests + coverage
./mvnw -T 1C test jacoco:report jacoco:report-aggregate \
  -Pcoverage,dsv1_2.12,dsv1_2.13,dsv2_3.1,dsv2_3.2,dsv2_3.3,dsv2_3.4,dsv2_3.5,dsv2_4.0,dsv2_4.1

# Integration tests (all profiles together — works on Java 17 in nightly because nothing is sharded)
./mvnw failsafe:integration-test failsafe:verify jacoco:report jacoco:report-aggregate \
  -Pcoverage,integration,dsv1_2.12,dsv1_2.13,dsv2_3.1,dsv2_3.2,dsv2_3.3,dsv2_3.4,dsv2_3.5,dsv2_4.0,dsv2_4.1

# Acceptance tests
./mvnw failsafe:integration-test failsafe:verify jacoco:report jacoco:report-aggregate \
  -Pcoverage,acceptance,dsv1_2.12,dsv1_2.13,dsv2_3.1,dsv2_3.2,dsv2_3.3,dsv2_3.4,dsv2_3.5,dsv2_4.0,dsv2_4.1
```

`-Drevision=0.0.<YYYYMMDD>` is set on each invocation in nightly, then the resulting jars are uploaded to `gs://spark-lib-nightly-snapshots/`.

---

## Local one-module shortcuts

Pick the profile that matches the connector you care about, then:

```bash
# Build only
./mvnw install -DskipTests -P<profile>

# Unit tests
./mvnw test -Pcoverage,<profile>

# Integration tests (env vars below required)
./mvnw failsafe:integration-test failsafe:verify -Pcoverage,integration,<profile>

# Acceptance tests
./mvnw failsafe:integration-test failsafe:verify -Pcoverage,acceptance,<profile>
```

Or scope to a single Maven module with `-pl`:
```bash
./mvnw -pl spark-bigquery-dsv2/spark-3.5-bigquery -am test -Pdsv2_3.5
```

---

## Required env vars

For integration tests:
| Var | What it is |
|---|---|
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to a service-account JSON or `gcloud auth login` credential. |
| `GOOGLE_CLOUD_PROJECT` | Test project ID. |
| `TEMPORARY_GCS_BUCKET` | GCS bucket for staging writes during integration tests. |
| `BIGLAKE_CONNECTION_ID` | Cloud Resource connection used by BigLake table tests. |
| `BIGQUERY_KMS_KEY_NAME` | KMS key for encrypted-table tests. |

Additional for acceptance tests:
| Var | What it is |
|---|---|
| `ACCEPTANCE_TEST_BUCKET` | GCS bucket reserved for acceptance scenarios. |
| `SERVERLESS_NETWORK_URI` | VPC network for serverless Dataproc batches. |

For Codecov upload (CI-only — skip locally):
| Var | What it is |
|---|---|
| `CODECOV_TOKEN` | Codecov upload token. |

---

## Common gotchas

- **Wrong JDK on integration tests** — Spark 3.1/3.2 and DSv1 (Scala 2.12/2.13) integration test stages need Java 8. Symptom: `UnsupportedClassVersionError` or Spark-internal `IllegalAccessError` on Java 17.
- **No profile selected** — `./mvnw test` with no profile only runs tests in the always-on common modules. If you expected to test a connector, you didn't.
- **DSv1 source is duplicated, not symlinked** — `spark-bigquery_2.12` and `spark-bigquery_2.13` keep near-identical but separate `src/main` and `src/test` trees. A fix in one does **not** automatically apply to the other; replicate it. Only `src/build` is shared via symlink.
- **Pushdown isn't in presubmit** — neither `presubmit.sh` nor `nightly.sh` invokes the `spark-bigquery-pushdown/` modules. Build/test those directly if you change them.
