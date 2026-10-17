package com.exchange.accounting.controller;

import com.exchange.accounting.service.AccountingService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Read-only API for the stateless accounting & tax read-model stored in Redis.
 */
@Tag(name = "Accounting & Tax",
        description = "Read-only accounting & tax read-model derived from settlement events. "
                + "Monetary values are plain fixed-point strings.")
@RestController
public class AccountingController {

    private final AccountingService accountingService;

    public AccountingController(AccountingService accountingService) {
        this.accountingService = accountingService;
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
            description = "Number of settled trades per symbol, read from the shared Redis aggregate.")
    @GetMapping("/accounting/trade-counts")
    public Map<String, Long> tradeCounts() {
        return accountingService.tradeCountBySymbol();
    }
}
