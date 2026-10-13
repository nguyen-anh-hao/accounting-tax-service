package com.exchange.accounting.controller;

import com.exchange.accounting.service.AccountingService;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Read-only API for the accounting & tax read-model and the RocksDB-backed
 * Kafka Streams view.
 */
@RestController
public class AccountingController {

    private final AccountingService accountingService;
    private final KafkaStreams streams;

    public AccountingController(AccountingService accountingService, KafkaStreams streams) {
        this.accountingService = accountingService;
        this.streams = streams;
    }

    @GetMapping("/accounting/summary")
    public Map<String, Object> summary() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("fee_revenue_by_symbol", accountingService.feeRevenueBySymbol());
        m.put("notional_by_symbol", accountingService.notionalBySymbol());
        m.put("cash_flow_by_account", accountingService.cashFlowByAccount());
        m.put("tax_liability_by_account", accountingService.taxLiabilityByAccount());
        return m;
    }

    @GetMapping("/accounting/trade-counts")
    public Map<String, Long> tradeCounts() {
        Map<String, Long> out = new LinkedHashMap<>();
        try {
            ReadOnlyKeyValueStore<String, Long> store = streams.store(
                    StoreQueryParameters.fromNameAndType("trade-count-by-symbol",
                            QueryableStoreTypes.keyValueStore()));
            store.all().forEachRemaining(kv -> out.put(kv.key, kv.value));
        } catch (InvalidStateStoreException e) {
            // Streams not yet (re)balanced; fall back to the in-memory view.
            out.putAll(accountingService.tradeCountBySymbol());
        }
        return out;
    }
}
