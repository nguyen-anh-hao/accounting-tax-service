package com.exchange.accounting;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class AccountingTaxApplication {
    public static void main(String[] args) {
        SpringApplication.run(AccountingTaxApplication.class, args);
    }
}
