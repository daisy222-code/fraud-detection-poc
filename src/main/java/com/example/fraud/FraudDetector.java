package com.example.fraud;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Processing-time rules; amounts are integer USD cents. State is isolated per account.
 */
public class FraudDetector extends KeyedProcessFunction<String, Transaction, Alert> {
    private transient ValueState<Long> smallPaymentExpiry;

    @Override
    public void open(OpenContext context) {
        smallPaymentExpiry = getRuntimeContext().getState(new ValueStateDescriptor<>("small-payment-expiry", Long.class));
    }

    @Override
    public void processElement(Transaction t, Context ctx, Collector<Alert> out) throws Exception {
        long now = ctx.timerService().currentProcessingTime();
        Long expiry = smallPaymentExpiry.value();
        if (t.amountCents >= 1_000_000) out.collect(new Alert(t, "HIGH_VALUE", now));
        if (expiry != null && now < expiry && t.amountCents >= 100_000) {
            out.collect(new Alert(t, "SMALL_THEN_LARGE", now));
            smallPaymentExpiry.clear();
        }
        if (t.amountCents < 100) {
            long next = now + 60_000;
            smallPaymentExpiry.update(next);
            ctx.timerService().registerProcessingTimeTimer(next);
        }
        LocalBridge.PROCESSED.incrementAndGet();
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        Long expiry = smallPaymentExpiry.value();
        if (expiry != null && expiry <= timestamp) smallPaymentExpiry.clear();
    }
}
