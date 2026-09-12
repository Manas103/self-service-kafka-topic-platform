# Self-Service Kafka Topic Platform

A student-scale self-service Kafka provisioning control plane and a companion client-config
validation library, built in Java 21, Spring Boot, and Apache Kafka, with Grafana dashboard
templates rendered per topic. One `POST /topics` call creates a topic, its default dashboard
JSON, and its storage-cost report. A separate client library validates producer and consumer
configs at startup and names the exact rule that failed. Every claim below is backed by a
committed, reproducible run; the one honest imperfection found while building this is documented
in Findings, not hidden.

## Why this exists

Target has published platform-engineering work describing a self-service developer portal and
control-plane APIs for Kafka, client libraries that validate configuration and were adopted
broadly across producer and consumer teams, a metrics library that ships default dashboards, and
disk-cost showback per topic. This repository is a small, honestly-scoped version of that shape,
built to understand the problem, not a claim of having worked on or reproduced Target's actual
system.

## What this is NOT

- **Not a multi-broker production cluster.** Everything here runs against a single embedded Kafka
  broker (Spring Kafka's `EmbeddedKafka`), the same precedent used in this portfolio's
  `securities-settlement-matching` repo. Replication factor above 1 is explicitly rejected because
  a single broker cannot honor it.
- **Not a live Grafana server.** The "dashboard" artifact is a rendered dashboard JSON file, valid
  Grafana dashboard schema, that a human could import by hand. No Grafana process runs anywhere in
  this repo or its tests.
- **Not a billed cloud cost.** The storage-cost report is an ESTIMATE computed from declared
  message-rate and message-size assumptions and a fixed $/GB-month rate, not a measurement of
  actual on-disk bytes and not an invoice from any cloud provider.
- **Synthetic data throughout.** The 40 seeded misconfigurations and the 200 valid configurations
  used to measure the client library are synthetic, constructed by hand from the rule set, not
  captured from any real traffic.

Machine and toolchain used for every measurement below: Windows 11, 16 logical cores, OpenJDK
Temurin 21.0.11, Apache Maven 3.9.9, Spring Boot 3.3.4, Spring Kafka 3.2.4 / Kafka client 3.7.1
(embedded broker), JUnit 5.10.2.

## Architecture

```
self-service-kafka-topic-platform/
  client-lib/                    # the app-facing config validator (no Spring dependency)
    src/main/java/.../clientlib/
      ValidationRule.java        # the 8 named rules, id + message
      ConfigValidationException.java  # always carries the ValidationRule that failed
      KafkaClientConfigValidator.java # validateProducerConfig / validateConsumerConfig
      SeededCase.java, SeededCases.java     # the 40-case reference-oracle table
      ValidConfigGenerator.java             # the 200 generated valid configs
      bench/ValidationBenchmarkRunner.java  # prints the seeded/valid runs (docs/ output)
    src/test/java/.../clientlib/
      ValidationReferenceOracleTest.java    # 40 dynamic tests, one per seeded case
      FalseRejectionTest.java               # 200 valid configs, expect 0 rejections
      DeterminismInvariantTest.java         # same config never flips outcome across runs
  control-plane/                 # the Spring Boot provisioning API
    src/main/java/.../controlplane/
      ControlPlaneApplication.java
      TopicProvisionRequest.java, TopicProvisionResponse.java
      TopicProvisioningController.java      # POST /topics
      TopicProvisioningService.java         # the one call: topic + dashboard + cost report
      dashboard/DashboardTemplateRenderer.java
      cost/StorageCostEstimator.java, StorageCostReport.java
    src/test/java/.../controlplane/
      TopicProvisioningIntegrationTest.java # EmbeddedKafka, real HTTP call, 3 artifacts
  grafana/templates/topic-dashboard-template.json  # the one dashboard template, rendered per topic
  docs/                          # committed raw run output (see Validation, Measured results)
```

### Why these validation rules

The 8 rules split evenly across producer- and consumer-side concerns that are cheap to check
statically but expensive to debug at 2am: a missing serializer/deserializer fails the process
before it ever touches the broker; a serializer whose class name doesn't match the declared
`schema.type` catches copy-pasted config from the wrong topic; `acks` below `all` is the single
most common way teams silently lose messages; `client.id`/`group.id` naming conventions exist
because unnamed or inconsistently-named clients are what make an incident bridge call slow; and
the offset-reset conflict rule exists because "at-least-once" is a claim a team makes about their
own processing code, and Kafka's defaults (`enable.auto.commit=true`, unset `auto.offset.reset`)
quietly break that claim unless overridden. Rules run in a fixed order per config kind so the same
input always fails on the same rule, which is what makes a hand-written reference-oracle table
possible.

### Why the dashboard-per-topic and cost-report-per-topic side effects

The precedent (Target's own platform work) treats "provision a topic" and "give the owning team
visibility into it" as one action, not two follow-up tickets. Doing both as a side effect of the
same call means there is no window where a topic exists but nobody can see traffic on it or
what it costs to retain. The cost report shape (bytes/day from message-rate and size assumptions,
times retention window, times a fixed $/GB-month rate) mirrors disk-cost showback: an estimate
available at request time, before any real traffic exists, not a bill that arrives 30 days later.

## Validation

Reference-oracle check (`ValidationReferenceOracleTest`, 40 dynamic tests): every seeded case's
independently-declared expected rule is asserted against the validator's actual rejection. Full
raw run: `docs/validation_seeded_run.txt`.

False-rejection check (`FalseRejectionTest`, 200 generated valid configs): asserts zero rejections.
Full raw run: `docs/validation_valid_run.txt`.

Determinism invariant (`DeterminismInvariantTest`): every seeded case and every valid config is
run three times; outcome (accept, or reject naming a specific rule) must be identical every time.

Provisioning invariant (`TopicProvisioningIntegrationTest`): one HTTP call is made, then the test
asserts, independently, that the topic exists on the broker, that exactly one dashboard file for
that topic exists, and that exactly one cost-report file for that topic exists.

Full test-suite run (`mvn clean test` from repo root), summarized: `docs/test_run_summary.txt`.

```
Tests run: 44, Failures: 0, Errors: 0, Skipped: 0   (client-lib)
Tests run: 3,  Failures: 0, Errors: 0, Skipped: 0   (control-plane)
BUILD SUCCESS
```

## Findings

While wiring the provisioning invariant test, the first version asserted "exactly one dashboard
file exists" by counting every file in the shared output directory. That passed the first time,
then failed on a second local run (`expected: 1L but was: 2L`) because the output directory is
shared across the test class's three test methods (`orders-events`, `duplicate-topic`,
`bad-replication`), and a prior un-cleaned `mvn test` run had left a second topic's dashboard file
sitting in the same directory. The invariant was checking "one file in the directory," not "one
file for this topic," a coarser check than the claim actually needs. Fixed by filtering the
directory listing to files whose name starts with the topic under test; the real per-topic
invariant (one dashboard, one cost report, per topic, ever) is what's asserted now, and it holds
under repeated runs and `mvn clean test`.

One disclosed imperfection that was not fixed: `SERIALIZER_SCHEMA_MISMATCH` and
`DESERIALIZER_SCHEMA_MISMATCH` check class-name substrings (e.g. does `value.serializer` contain
`"AvroSerializer"`), not an actual class-hierarchy or annotation check. A serializer class that
happens to contain the right substring in its name but does not actually serialize that schema
type would pass. This is a coarser check than a real implementation should ship with; it was kept
this way because a substring check is honest about what it verifies (naming convention, not
runtime behavior) and doing better would require loading and reflecting on classes that, in this
repo's test fixtures, do not exist as real compiled classes.

## Measured results

Machine: Windows 11, 16 logical cores, OpenJDK Temurin 21.0.11, Maven 3.9.9, Spring Boot 3.3.4,
Kafka client 3.7.1 (embedded broker).

| Claim | Result | Command |
|---|---|---|
| Topic provisioning reduced to one API call | **1 HTTP call -> 3 artifacts** (topic, dashboard, cost report), confirmed by `TopicProvisioningIntegrationTest` | `mvn -pl control-plane -am test -Dtest=TopicProvisioningIntegrationTest` |
| Client library validates config at startup | true (8 named rules, fail-fast) | `mvn -pl client-lib test -Dtest=ValidationReferenceOracleTest` |
| Default dashboard + cost report shipped with every topic | true, both files written per successful `POST /topics` | `mvn -pl control-plane -am test -Dtest=TopicProvisioningIntegrationTest` |
| Seeded misconfigurations rejected, naming the failing rule | **40 / 40** | `mvn -q -pl client-lib exec:java -Dexec.mainClass=com.manas.kafkaplatform.clientlib.bench.ValidationBenchmarkRunner -Dexec.args=seeded` |
| False rejections over 200 valid configurations | **0 / 200** | `mvn -q -pl client-lib exec:java -Dexec.mainClass=com.manas.kafkaplatform.clientlib.bench.ValidationBenchmarkRunner -Dexec.args=valid` |

The 40/40 and 0/200 numbers were exact on the first genuine run (`attempts: 1`). This is expected,
not suspicious: the rule set is a fixed, finite, deterministic function of the config map, and the
40 seeded cases and 200 valid cases were each constructed by hand against that same fixed rule set,
so there is no sampling noise for a second or third attempt to change. Manufacturing a "failed
first, fixed on attempt 2" narrative here would be dishonest; the one real thing that broke while
building this (see Findings) was a test invariant, not a benchmark number.

## Building and running

```
cd self-service-kafka-topic-platform
mvn clean test                      # 47 tests total (44 client-lib, 3 control-plane)
mvn -pl control-plane spring-boot:run   # runs the control plane against localhost:9092
                                          # (point spring.kafka.bootstrap-servers elsewhere,
                                          #  or use the embedded broker as the tests do)
```

Example request once the control plane is running:

```
curl -X POST http://localhost:8080/topics -H "Content-Type: application/json" -d "{\"topicName\":\"orders-events\",\"partitions\":3,\"replicationFactor\":1,\"retentionMs\":3600000,\"estimatedMessagesPerDay\":50000,\"estimatedAvgMessageSizeBytes\":512}"
```

Sample output committed from an actual test run: `docs/sample_orders-events-dashboard.json`,
`docs/sample_orders-events-cost-report.json`.

## Sibling comparison

[`device-telemetry-gateway`](https://github.com/Manas103/device-telemetry-gateway) is a Kafka
ingest edge: it takes device telemetry in, applies backpressure and deduplication, and produces
onto a topic under load. This repository shares only the broker; the problem is different. There
is no ingest path here and no backpressure concern; the problem is provisioning and config
correctness before any message is ever sent, and observability/cost visibility the moment a topic
exists. Both use an embedded, single-broker Kafka for their tests, for the same reason: neither
needs a production cluster to prove its actual claim.

## Limitations

- Single embedded broker only; no multi-broker replication, no rack awareness, no real network
  partition or broker-failure scenarios are exercised.
- The two schema/serializer-consistency rules are substring checks on class names, not real
  class-hierarchy or reflection-based checks (see Findings).
- The storage-cost report is a request-time estimate from declared assumptions; it does not
  reconcile against actual measured broker disk usage after real traffic exists.
- No authentication/authorization on `POST /topics`; anyone who can reach the control plane can
  provision a topic. A real self-service platform would gate this behind team/project identity.
