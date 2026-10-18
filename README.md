# accounting-tax-service

## Purpose

Builds a read-only accounting and tax view from settlement events. It aggregates fee revenue, notional, trade counts, cash flow and tax liability for reporting. It is not the ledger and cannot move money; every number is derived from events produced elsewhere.

## Domain knowledge

Fee revenue per symbol is the sum of buyer and seller fees, and notional is price times quantity. Trade count is the number of settled trades per symbol. Tax liability is a flat percentage of collected fees attributed to the seller account, which makes it a placeholder model rather than a jurisdiction-aware tax engine.

Cash flow is counted only from cash ledger rows, as credit minus debit. Deposit and withdrawal events are deliberately ignored for that aggregate, because the same movement already appears as a ledger row and counting both would double every amount. All aggregates live in Redis hashes as integers scaled by one hundred million, updated with atomic increments so replicas share one view and no floating point enters the totals.

The view is eventually consistent and read-only. It starts from the earliest offset, commits after processing each record, and uses retries plus a dead-letter topic for poison messages. It offers no drill-down beyond the five aggregate families it maintains.

## Decisions and consequences

**CQRS read model.** Aggregates are separated from the ledger and optimized for reporting. The benefit is a small, fast view; the cost is eventual consistency and no ability to answer questions the aggregates were not designed for.

**Event-carried state transfer.** Each event carries the amounts needed for aggregation, so the consumer never calls back into settlement. This keeps the read model independent, at the price of schema coupling.

**Fixed-point integers in Redis.** Atomic integer increments are exact and safe across replicas. The cost is explicit scaling at the boundary and an upper bound imposed by 64-bit integers.

**Redis as the only aggregate store.** Any replica can serve reads and a restart loses nothing. The cost is a hard dependency on Redis and no historical query beyond the current aggregate values.

**Incrementing consumer without per-trade dedupe.** Offsets commit after processing, which protects against loss. A crash between the Redis increment and the offset commit can still double-count, so the aggregates are not replay-safe today.

## Operations and known limits

The service needs Kafka, a schema registry and Redis; without them the consumer cannot make progress. The smoke publisher lives in test sources and is never packaged into the jar, so producing sample events requires running it from the IDE or Maven. Swagger and the two aggregate endpoints are exposed directly on port 8086 rather than through the edge.

The most important limitation is replay behavior: aggregates are increments, not idempotent upserts, so rebuilding after a bad deployment would require clearing the Redis hashes and replaying the topics from the beginning. Dead-letter topics catch poison records but do not repair aggregates already double-counted. The tax rate is a single flat configuration value applied to every symbol and account.

The consumer group is shared by all replicas, so scaling out adds parallel consumption rather than duplicate work. Because processing is per record, a single malformed payload blocks its partition until retries are exhausted and the record reaches the dead-letter topic. Aggregate keys never expire, so Redis memory grows with the number of traded symbols and accounts.

## Future direction

No committed roadmap exists for this service. The first technical step before trusting it for reporting would be per-trade idempotency and a documented rebuild procedure.

Business evolution would add jurisdiction-specific tax rules, period close and historical snapshots, which are product decisions rather than code changes. If the view becomes authoritative, it should first gain an auditable source-of-truth store instead of Redis-only aggregates.
