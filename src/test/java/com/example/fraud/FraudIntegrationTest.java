package com.example.fraud;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class FraudIntegrationTest {
    @Autowired
    TestRestTemplate http;
    @Autowired
    FlinkEngine engine;

    @Test
    void realFlinkPipelineAndValidation() {
        await().atMost(Duration.ofSeconds(30)).until(() -> "RUNNING".equals(engine.status()));
        assertThat(http.postForEntity("/api/transactions", new Transaction("bad", "A", -1), String.class).getStatusCode().value()).isEqualTo(400);
        send("small", "A", 50);
        send("other-account", "B", 150000);
        send("large", "A", 150000);
        send("normal", "C", 5000);
        send("high", "D", 1000000);
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(LocalBridge.PROCESSED.get()).isEqualTo(5);
            assertThat(LocalBridge.alerts()).extracting(a -> a.transactionId + ":" + a.rule)
                    .containsExactlyInAnyOrder("large:SMALL_THEN_LARGE", "high:HIGH_VALUE");
        });
    }

    private void send(String id, String account, long amount) {
        assertThat(http.postForEntity("/api/transactions", new Transaction(id, account, amount), String.class).getStatusCode().value()).isEqualTo(202);
    }
}
