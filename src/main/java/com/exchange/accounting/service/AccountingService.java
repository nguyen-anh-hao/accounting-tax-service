package com.exchange.accounting.service;

import com.exchange.accounting.LedgerEntryRecordedEvent;
import com.exchange.accounting.TradeSettledEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory accounting & tax read-model derived from the settlement events
 * streamed over Kafka. Demonstrates the consumer side; in production this state
 * would be persisted to a dedicated accounting database.
 *
 * <p>All monetary arithmetic uses {@link BigDecimal} (fixed-point) — no floating
 * point — consistent with the rest of the platform.</p>
 *
 * <p><b>Cash flow source of truth:</b> the double-entry journal (the CASH rows of
 * {@link LedgerEntryRecordedEvent}). For a single movement clearing-settlement
 * emits a Deposit/Withdrawal event <i>and</i> the matching ledger rows, so
 * counting both sources would double every amount — {@code cash_flow_by_account}
 * is therefore derived from the ledger only.</p>
 */
@Service
public class AccountingService {

    private final BigDecimal taxRate;

    private final Map<String, BigDecimal> feeRevenueBySymbol = new ConcurrentHashMap<>();
    private final Map<String, BigDecimal> notionalBySymbol = new ConcurrentHashMap<>();
    private final Map<String, Long> tradeCountBySymbol = new ConcurrentHashMap<>();
    private final Map<Long, BigDecimal> cashFlowByAccount = new ConcurrentHashMap<>();
    private final Map<Long, BigDecimal> taxLiabilityByAccount = new ConcurrentHashMap<>();

    public AccountingService(@Value("${app.accounting.tax-rate:0.10}") BigDecimal taxRate) {
        this.taxRate = taxRate;
    }

    public void onTradeSettled(TradeSettledEvent e) {
        String symbol = e.getSymbol();
        BigDecimal fee = new BigDecimal(e.getBuyerFee()).add(new BigDecimal(e.getSellerFee()));
        BigDecimal notional = new BigDecimal(e.getPrice()).multiply(new BigDecimal(e.getQuantity()));

        feeRevenueBySymbol.merge(symbol, fee, BigDecimal::add);
        notionalBySymbol.merge(symbol, notional, BigDecimal::add);
        tradeCountBySymbol.merge(symbol, 1L, Long::sum);

        // Placeholder tax model: flat tax on the fee revenue (charged to the seller).
        BigDecimal tax = fee.multiply(taxRate);
        taxLiabilityByAccount.merge(e.getSellerAccountId(), tax, BigDecimal::add);
    }

    /**
     * Cash movement per account, derived from the double-entry journal. Every cash
     * movement (deposit, withdrawal, trade leg) produces a CASH ledger row, so this
     * is the single, un-doubled source for {@code cash_flow_by_account}.
     */
    public void onLedgerEntry(LedgerEntryRecordedEvent e) {
        if ("CASH".equals(e.getAsset())) {
            BigDecimal delta = new BigDecimal(e.getCredit()).subtract(new BigDecimal(e.getDebit()));
            cashFlowByAccount.merge(e.getAccountId(), delta, BigDecimal::add);
        }
    }

    public Map<String, BigDecimal> feeRevenueBySymbol() {
        return feeRevenueBySymbol;
    }

    public Map<String, BigDecimal> notionalBySymbol() {
        return notionalBySymbol;
    }

    public Map<String, Long> tradeCountBySymbol() {
        return tradeCountBySymbol;
    }

    public Map<Long, BigDecimal> cashFlowByAccount() {
        return cashFlowByAccount;
    }

    public Map<Long, BigDecimal> taxLiabilityByAccount() {
        return taxLiabilityByAccount;
    }
}
