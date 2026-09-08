# Real-time fraud detection POC

Spring Boot 3.5.16 + Apache Flink 1.20.3, Java 17, Maven.

## Run locally

The project is open in IntelliJ IDEA. In its Terminal, run:

```sh
./run.sh
```

This builds and starts the API at http://localhost:8080. Stop with Ctrl+C.
The local `mvnw` convenience launcher uses IntelliJ's bundled Maven (or Maven on PATH), and the downloaded Java 17 in `.tools/amazon-corretto-17.jdk/Contents/Home`. It is not the standard Maven Wrapper. On another machine install Maven and Java 17 and set JAVA_HOME.

For editor Run/Debug, set Project Structure → SDK → Add JDK to the above Java home, import `pom.xml` as Maven, and select **Fraud Detection POC**. The saved configuration includes the Java module options required by the local Flink runtime.

## Try the demo

Use `requests.http` in IntelliJ's HTTP client, or these commands in another terminal:

```sh
curl http://localhost:8080/api/status
curl -X POST http://localhost:8080/api/demo
curl http://localhost:8080/api/alerts
```

Wait for status `RUNNING` before submitting. Demo submits four payments and yields two alerts asynchronously. Poll alerts if processing has not completed. Each demo uses distinct account IDs.

| Rule | Trigger |
| --- | --- |
| HIGH_VALUE | Payment of at least USD 10,000 |
| SMALL_THEN_LARGE | Payment below USD 1 followed by at least USD 1,000 on the same account within 60 seconds |

A payment can trigger both rules. All amounts are integer USD cents. The 60-second interval uses Flink processing time, not a client-supplied timestamp. A new small payment refreshes the interval; a qualifying large payment consumes it. Other payments do not clear the interval.

## Architecture

REST POST → bounded in-memory queue → actual local Flink streaming job → keyBy(accountId) → keyed state and timers → bounded recent-alert store → REST GET.

- `FraudController`: validation, submissions, demo, alerts, status.
- `FlinkEngine`: embedded Flink lifecycle and local source/sink.
- `FraudDetector`: fraud rules with per-account ValueState and expiry timers.
- `LocalBridge`: single-JVM transport, 10,000 queued payments, last 1,000 alerts.

## Verification

```sh
./mvnw verify
```

Integration test starts Spring Boot and a real Flink job, checks both rules, account isolation, normal payments, and invalid amounts. Build and integration test passed locally on Java 17.

## POC boundaries

All data disappears on restart. No durable replay, deduplication, authentication, or exactly-once guarantees. Repeated transaction IDs are processed again. HTTP 202 means queued, not that fraud evaluation has completed. Queue overload returns 429 and unavailable Flink returns 503. Demo submission is not atomic if capacity runs out. The bridge only works in this one JVM, with parallelism 1. The API binds to loopback. Use synthetic data.

For a production evolution, replace the bridge with Kafka and a persistent alert sink, deploy Flink separately, add checkpoint/recovery testing, event-time and late-event policy, idempotency, authentication, monitoring, and rule configuration. This POC flags sample patterns; it does not make real payment decisions.

References: [Flink keyed state](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/fault-tolerance/state/), [Spring Boot documentation](https://docs.spring.io/spring-boot/3.5/reference/index.html).

Run through `run.sh` or IntelliJ's application configuration. The Spring Boot nested executable JAR is not a supported launcher for this embedded Flink POC because Flink requires dependencies on the ordinary JVM classpath.
