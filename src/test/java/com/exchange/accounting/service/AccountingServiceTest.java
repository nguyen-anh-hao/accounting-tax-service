package com.exchange.accounting.service;

import com.exchange.accounting.DepositEvent;
import com.exchange.accounting.LedgerEntryRecordedEvent;
import com.exchange.accounting.TradeSettledEvent;
import com.exchange.accounting.WithdrawalEvent;
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
    void onDeposit_and_onWithdrawal_updateCashFlow() {
        service.onDeposit(DepositEvent.newBuilder()
                .setAccountId(1001L).setAsset("CASH").setAmount("10000.00")
                .setReference("").setTimestampNs(0L)
                .build());
        service.onWithdrawal(WithdrawalEvent.newBuilder()
                .setAccountId(1001L).setAsset("CASH").setAmount("2500.00")
                .setReference("").setTimestampNs(0L)
                .build());

        assertThat(service.cashFlowByAccount().get(1001L)).isEqualByComparingTo("7500.00");
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
