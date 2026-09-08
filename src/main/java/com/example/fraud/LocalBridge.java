package com.example.fraud;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Single-JVM demo transport. Not a durable or distributed source/sink.
 */
public final class LocalBridge {
    static final BlockingQueue<Transaction> INPUT = new ArrayBlockingQueue<>(10000);
    static final AtomicLong PROCESSED = new AtomicLong();
    private static final Deque<Alert> ALERTS = new ArrayDeque<>();

    static synchronized void add(Alert alert) {
        if (ALERTS.size() >= 1000) ALERTS.removeLast();
        ALERTS.addFirst(alert);
    }

    static synchronized List<Alert> alerts() {
        return List.copyOf(ALERTS);
    }
}
