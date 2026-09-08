package com.example.fraud;

import jakarta.validation.constraints.*;

import java.io.Serializable;

public class Transaction implements Serializable {
    @NotBlank
    @Size(max = 100)
    public String transactionId;
    @NotBlank
    @Size(max = 100)
    public String accountId;
    @Min(1)
    @Max(100000000000L)
    public long amountCents;

    public Transaction() {
    }

    public Transaction(String id, String account, long cents) {
        transactionId = id;
        accountId = account;
        amountCents = cents;
    }
}
