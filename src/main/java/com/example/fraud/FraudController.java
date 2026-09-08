package com.example.fraud;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.*;

@RestController
@RequestMapping("/api")
public class FraudController {
    private final FlinkEngine engine;

    public FraudController(FlinkEngine engine) {
        this.engine = engine;
    }

    @PostMapping("/transactions")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, String> submit(@Valid @RequestBody Transaction t) {
        if (!"RUNNING".equals(engine.status()))
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Flink is not running");
        if (!LocalBridge.INPUT.offer(t))
            throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS, "Transaction queue is full");
        return Map.of("status", "ACCEPTED", "transactionId", t.transactionId);
    }

    @GetMapping("/alerts")
    public List<Alert> alerts() {
        return LocalBridge.alerts();
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        return Map.of("flink", engine.status(), "queued", LocalBridge.INPUT.size(), "processed", LocalBridge.PROCESSED.get());
    }

    @PostMapping("/demo")
    public Map<String, Object> demo() {
        String prefix = UUID.randomUUID().toString();
        for (var t : List.of(new Transaction(prefix + "-normal", prefix + "-A", 4200), new Transaction(prefix + "-small", prefix + "-B", 50), new Transaction(prefix + "-large", prefix + "-B", 150000), new Transaction(prefix + "-high", prefix + "-C", 1500000)))
            submit(t);
        return Map.of("submitted", 4, "expectedAlerts", 2, "batch", prefix);
    }
}
