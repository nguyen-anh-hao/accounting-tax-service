package com.exchange.accounting.service;

import com.exchange.accounting.DepositEvent;
import com.exchange.accounting.LedgerEntryRecordedEvent;
import com.exchange.accounting.TradeSettledEvent;
import com.exchange.accounting.WithdrawalEvent;
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

    public void onLedgerEntry(LedgerEntryRecordedEvent e) {
        if ("CASH".equals(e.getAsset())) {
            BigDecimal delta = new BigDecimal(e.getCredit()).subtract(new BigDecimal(e.getDebit()));
            cashFlowByAccount.merge(e.getAccountId(), delta, BigDecimal::add);
        }
    }

    public void onDeposit(DepositEvent e) {
        cashFlowByAccount.merge(e.getAccountId(), new BigDecimal(e.getAmount()), BigDecimal::add);
    }

    public void onWithdrawal(WithdrawalEvent e) {
        cashFlowByAccount.merge(e.getAccountId(), new BigDecimal(e.getAmount()).negate(), BigDecimal::add);
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
