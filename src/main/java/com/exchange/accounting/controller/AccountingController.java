package com.exchange.accounting.controller;

import com.exchange.accounting.service.AccountingService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Read-only API for the accounting & tax read-model and the RocksDB-backed
 * Kafka Streams view.
 */
@Tag(name = "Accounting & Tax",
        description = "Read-only accounting & tax read-model derived from settlement events. "
                + "Monetary values are plain fixed-point strings.")
@RestController
public class AccountingController {

    private static final Logger log = LoggerFactory.getLogger(AccountingController.class);

    private final AccountingService accountingService;
    private final KafkaStreams streams;

    public AccountingController(AccountingService accountingService, KafkaStreams streams) {
        this.accountingService = accountingService;
        this.streams = streams;
    }

    @Operation(summary = "Accounting summary",
            description = "Aggregated fee revenue and notional per symbol, plus cash-flow and "
                    + "tax liability per account, derived from the settlement event stream.")
    @GetMapping("/accounting/summary")
    public Map<String, Object> summary() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("fee_revenue_by_symbol", accountingService.feeRevenueBySymbol());
        m.put("notional_by_symbol", accountingService.notionalBySymbol());
        m.put("cash_flow_by_account", accountingService.cashFlowByAccount());
        m.put("tax_liability_by_account", accountingService.taxLiabilityByAccount());
        return m;
    }

    @Operation(summary = "Trade count per symbol",
            description = "Number of settled trades per symbol, read from the RocksDB-backed "
                    + "Kafka Streams materialized view. Falls back to the in-memory view while "
                    + "the streams app is rebalancing.")
    @GetMapping("/accounting/trade-counts")
    public Map<String, Long> tradeCounts() {
        Map<String, Long> out = new LinkedHashMap<>();
        try {
            ReadOnlyKeyValueStore<String, Long> store = streams.store(
                    StoreQueryParameters.fromNameAndType("trade-count-by-symbol",
                            QueryableStoreTypes.keyValueStore()));
            store.all().forEachRemaining(kv -> out.put(kv.key, kv.value));
        } catch (IllegalStateException | StreamsException e) {
            // The state store is not queryable: Streams may still be (re)balancing, or
            // the client is in ERROR state (e.g. a source topic was missing at startup).
            // Serve the in-memory view instead of failing the whole request with a 500.
            log.warn("trade-count state store unavailable ({}); serving in-memory view", e.getMessage());
            out.putAll(accountingService.tradeCountBySymbol());
        }
        return out;
    }
}
