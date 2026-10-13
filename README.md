# Accounting & Tax (`accounting-tax-service`)

> **Accounting & Tax read-model** built with Java 21 & Spring Boot 3. It consumes
> settlement events from Kafka (relayed via the Transactional Outbox pattern +
> Debezium CDC) and derives fee revenue, notional, cash-flow and tax liability.

---

## 1. Primary Responsibility
This service is the downstream accounting/tax consumer of the financial events
produced by `clearing-settlement`. It does **not** own the ledger; it derives a
read-only accounting view for reporting and tax purposes.

## 2. Boundaries & I/O
- **Ingress (Kafka, Avro + Schema Registry):**
  - `accounting.trade` → `TradeSettledEvent`
  - `accounting.ledger` → `LedgerEntryRecordedEvent`
  - `accounting.deposit` → `DepositEvent`
  - `accounting.withdrawal` → `WithdrawalEvent`
- **Egress (REST):** `GET /accounting/summary`, `GET /accounting/trade-counts`.
- **State:** in-memory read-model + embedded **RocksDB** state store via Kafka Streams.

## 3. Consumer guarantees (strict / controlled)
- Avro + Confluent Schema Registry, `specific.avro.reader=true`.
- `enable.auto.commit=false` — offsets committed only after each record is processed.
- `isolation.level=read_committed`.
- Retry (3x, 1s fixed backoff) + dead-letter topic (`<topic>.DLT`) for poison messages.

## 4. Quick Start
```bash
# Prerequisites: JDK 21, Maven, running Kafka (Redpanda) + Schema Registry.
mvn spring-boot:run
```

## 5. Event contracts
Avro schemas live in `src/main/avro/accounting/` (vendored from `trading-contracts/avro/`).
