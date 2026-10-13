package com.exchange.accounting.consumer;

import com.exchange.accounting.DepositEvent;
import com.exchange.accounting.LedgerEntryRecordedEvent;
import com.exchange.accounting.TradeSettledEvent;
import com.exchange.accounting.WithdrawalEvent;
import com.exchange.accounting.service.AccountingService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes accounting events from the settlement outbox (relayed by Debezium)
 * and applies them to the accounting & tax read-model. Offsets are committed by
 * Spring only after each record is successfully processed (auto-commit off).
 */
@Component
public class AccountingEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(AccountingEventConsumer.class);

    private final AccountingService accountingService;

    public AccountingEventConsumer(AccountingService accountingService) {
        this.accountingService = accountingService;
    }

    @KafkaListener(topics = "accounting.trade", groupId = "accounting-tax-service")
    public void onTradeSettled(ConsumerRecord<String, Object> record) {
        TradeSettledEvent event = (TradeSettledEvent) record.value();
        accountingService.onTradeSettled(event);
        log.debug("consumed trade event trade_id={}", event.getTradeId());
    }

    @KafkaListener(topics = "accounting.ledger", groupId = "accounting-tax-service")
    public void onLedgerEntry(ConsumerRecord<String, Object> record) {
        LedgerEntryRecordedEvent event = (LedgerEntryRecordedEvent) record.value();
        accountingService.onLedgerEntry(event);
        log.debug("consumed ledger entry entry_id={}", event.getEntryId());
    }

    @KafkaListener(topics = "accounting.deposit", groupId = "accounting-tax-service")
    public void onDeposit(ConsumerRecord<String, Object> record) {
        DepositEvent event = (DepositEvent) record.value();
        accountingService.onDeposit(event);
        log.debug("consumed deposit account_id={}", event.getAccountId());
    }

    @KafkaListener(topics = "accounting.withdrawal", groupId = "accounting-tax-service")
    public void onWithdrawal(ConsumerRecord<String, Object> record) {
        WithdrawalEvent event = (WithdrawalEvent) record.value();
        accountingService.onWithdrawal(event);
        log.debug("consumed withdrawal account_id={}", event.getAccountId());
    }
}
