package com.exchange.accounting.service;

import com.exchange.accounting.LedgerEntryRecordedEvent;
import com.exchange.accounting.TradeSettledEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class AccountingServiceTest {

    private AccountingService service;

    @BeforeEach
    void setUp() {
        service = new AccountingService(new BigDecimal("0.10"));
    }

    @Test
    void onTradeSettled_accumulatesFeesNotionalCountAndTax() {
        service.onTradeSettled(trade("AAPL", "150.00", "10", "1.50", "1.50", 1002L));

        assertThat(service.feeRevenueBySymbol().get("AAPL")).isEqualByComparingTo("3.00");
        assertThat(service.notionalBySymbol().get("AAPL")).isEqualByComparingTo("1500.00");
        assertThat(service.tradeCountBySymbol()).containsEntry("AAPL", 1L);
        // tax = fee * 0.10 = 0.30, charged to the seller.
        assertThat(service.taxLiabilityByAccount().get(1002L)).isEqualByComparingTo("0.30");
    }

    @Test
    void onTradeSettled_multipleTradesAccumulate() {
        service.onTradeSettled(trade("AAPL", "100", "10", "1.00", "1.00", 1002L));
        service.onTradeSettled(trade("AAPL", "200", "5", "1.00", "1.00", 1002L));

        assertThat(service.tradeCountBySymbol()).containsEntry("AAPL", 2L);
        assertThat(service.notionalBySymbol().get("AAPL")).isEqualByComparingTo("2000.00");
        assertThat(service.feeRevenueBySymbol().get("AAPL")).isEqualByComparingTo("4.00");
    }

    @Test
    void onLedgerEntry_onlyCashMovesCashFlow() {
        service.onLedgerEntry(ledger(1L, 1001L, "CASH", "150.00", "0", "BUY"));
        service.onLedgerEntry(ledger(2L, 1001L, "STOCK:AAPL", "0", "10", "BUY"));

        // Only the CASH row moves cash flow: credit - debit = 0 - 150 = -150.
        assertThat(service.cashFlowByAccount().get(1001L)).isEqualByComparingTo("-150.00");
    }

    @Test
    void cashFlow_isLedgerDriven_andNotDoubleCounted() {
        // Mirrors what clearing-settlement writes for one deposit + one withdrawal:
        // each movement produces a CASH row for the user account AND the house
        // account (0). Cash flow must follow the ledger only (no Deposit/Withdrawal
        // event), otherwise every amount is counted twice.
        service.onLedgerEntry(ledger(1L, 1001L, "CASH", "0", "10000.00", "DEPOSIT"));
        service.onLedgerEntry(ledger(2L, 0L, "CASH", "10000.00", "0", "DEPOSIT"));
        service.onLedgerEntry(ledger(3L, 1001L, "CASH", "500.00", "0", "WITHDRAWAL"));
        service.onLedgerEntry(ledger(4L, 0L, "CASH", "0", "500.00", "WITHDRAWAL"));

        assertThat(service.cashFlowByAccount().get(1001L)).isEqualByComparingTo("9500.00");
        assertThat(service.cashFlowByAccount().get(0L)).isEqualByComparingTo("-9500.00");
    }

    private static TradeSettledEvent trade(String symbol, String price, String qty,
                                           String buyerFee, String sellerFee, long sellerAccountId) {
        return TradeSettledEvent.newBuilder()
                .setTradeId(1L)
                .setSymbol(symbol)
                .setPrice(price)
                .setQuantity(qty)
                .setBuyerFee(buyerFee)
                .setSellerFee(sellerFee)
                .setMakerOrderId(0L)
                .setTakerOrderId(0L)
                .setMakerAccountId(0L)
                .setTakerAccountId(0L)
                .setBuyerAccountId(0L)
                .setSellerAccountId(sellerAccountId)
                .setTimestampNs(0L)
                .build();
    }

    private static LedgerEntryRecordedEvent ledger(long entryId, long accountId, String asset,
                                                   String debit, String credit, String reason) {
        return LedgerEntryRecordedEvent.newBuilder()
                .setEntryId(entryId)
                .setAccountId(accountId)
                .setAsset(asset)
                .setDebit(debit)
                .setCredit(credit)
                .setReference("")
                .setReason(reason)
                .setTimestampNs(0L)
                .build();
    }
}
