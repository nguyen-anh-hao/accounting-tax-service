package com.exchange.accounting.service;

import com.exchange.accounting.LedgerEntryRecordedEvent;
import com.exchange.accounting.TradeSettledEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Stateless accounting & tax read-model. Aggregates live in Redis hashes and are
 * updated with atomic HINCRBY operations, so all replicas share one view and a
 * restart loses nothing.
 *
 * <p>Monetary values are stored as {@code long} scaled by 1e8 (fixed-point,
 * 8 decimals). HINCRBY is exact for integers, avoiding the precision loss of
 * floating point.</p>
 */
@Service
public class AccountingService {

    private static final BigDecimal SCALE = BigDecimal.valueOf(100_000_000L);
    private static final int SCALE_DIGITS = 8;

    private static final String FEE_REVENUE = "accounting:fee_revenue";
    private static final String NOTIONAL = "accounting:notional";
    private static final String TRADE_COUNT = "accounting:trade_count";
    private static final String CASH_FLOW = "accounting:cash_flow";
    private static final String TAX_LIABILITY = "accounting:tax_liability";

    private final StringRedisTemplate redis;
    private final BigDecimal taxRate;

    public AccountingService(StringRedisTemplate redis,
                             @Value("${app.accounting.tax-rate:0.10}") BigDecimal taxRate) {
        this.redis = redis;
        this.taxRate = taxRate;
    }

    public void onTradeSettled(TradeSettledEvent e) {
        String symbol = e.getSymbol();
        BigDecimal fee = new BigDecimal(e.getBuyerFee()).add(new BigDecimal(e.getSellerFee()));
        BigDecimal notional = new BigDecimal(e.getPrice()).multiply(new BigDecimal(e.getQuantity()));
        BigDecimal tax = fee.multiply(taxRate);

        redis.opsForHash().increment(FEE_REVENUE, symbol, scaled(fee));
        redis.opsForHash().increment(NOTIONAL, symbol, scaled(notional));
        redis.opsForHash().increment(TRADE_COUNT, symbol, 1L);
        redis.opsForHash().increment(TAX_LIABILITY, String.valueOf(e.getSellerAccountId()), scaled(tax));
    }

    /**
     * Cash movement per account, derived from the double-entry journal. Every cash
     * movement (deposit, withdrawal, trade leg) produces a CASH ledger row, so this
     * is the single, un-doubled source for {@code cash_flow_by_account}.
     */
    public void onLedgerEntry(LedgerEntryRecordedEvent e) {
        if ("CASH".equals(e.getAsset())) {
            BigDecimal delta = new BigDecimal(e.getCredit()).subtract(new BigDecimal(e.getDebit()));
            redis.opsForHash().increment(CASH_FLOW, String.valueOf(e.getAccountId()), scaled(delta));
        }
    }

    public Map<String, BigDecimal> feeRevenueBySymbol() {
        return moneyByStringKey(FEE_REVENUE);
    }

    public Map<String, BigDecimal> notionalBySymbol() {
        return moneyByStringKey(NOTIONAL);
    }

    public Map<String, Long> tradeCountBySymbol() {
        Map<String, Long> out = new LinkedHashMap<>();
        redis.opsForHash().entries(TRADE_COUNT)
                .forEach((k, v) -> out.put(String.valueOf(k), Long.parseLong(String.valueOf(v))));
        return out;
    }

    public Map<Long, BigDecimal> cashFlowByAccount() {
        return moneyByAccountKey(CASH_FLOW);
    }

    public Map<Long, BigDecimal> taxLiabilityByAccount() {
        return moneyByAccountKey(TAX_LIABILITY);
    }

    private Map<String, BigDecimal> moneyByStringKey(String key) {
        Map<String, BigDecimal> out = new LinkedHashMap<>();
        redis.opsForHash().entries(key)
                .forEach((k, v) -> out.put(String.valueOf(k), unscaled(Long.parseLong(String.valueOf(v)))));
        return out;
    }

    private Map<Long, BigDecimal> moneyByAccountKey(String key) {
        Map<Long, BigDecimal> out = new LinkedHashMap<>();
        redis.opsForHash().entries(key)
                .forEach((k, v) -> out.put(Long.parseLong(String.valueOf(k)),
                        unscaled(Long.parseLong(String.valueOf(v)))));
        return out;
    }

    private static long scaled(BigDecimal value) {
        return value.multiply(SCALE).setScale(0, RoundingMode.HALF_UP).longValueExact();
    }

    private static BigDecimal unscaled(long value) {
        return BigDecimal.valueOf(value, SCALE_DIGITS);
    }
}
