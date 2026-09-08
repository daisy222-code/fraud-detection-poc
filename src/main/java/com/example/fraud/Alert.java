package com.example.fraud;

import java.io.Serializable;

public class Alert implements Serializable {
    public String transactionId;
    public String accountId;
    public String rule;
    public long amountCents;
    public long detectedAt;

    public Alert() {
    }

    public Alert(Transaction t, String reason, long now) {
        transactionId = t.transactionId;
        accountId = t.accountId;
        amountCents = t.amountCents;
        rule = reason;
        detectedAt = now;
    }
}
