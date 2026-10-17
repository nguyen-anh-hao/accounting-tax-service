package com.exchange.accounting.tools;

import com.exchange.accounting.LedgerEntryRecordedEvent;
import com.exchange.accounting.TradeSettledEvent;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Dev-only smoke test: publishes a sample trade plus the CASH ledger rows that
 * back a deposit and a withdrawal, so the read-model behind
 * {@code /accounting/summary} and {@code /accounting/trade-counts} has data to show.
 *
 * <p>This lives in {@code src/test} on purpose — it is not part of the service
 * and never ends up in the packaged jar. Run it <b>from your IDE</b> (just run
 * {@code main}), or from the command line:</p>
 *
 * <pre>mvn exec:java -Dexec.mainClass=com.exchange.accounting.tools.SmokeEventPublisher</pre>
 *
 * <p>It targets the standalone dev infrastructure started by
 * {@code deployments/docker/docker-compose.dev.yml}, using the same defaults as
 * the service: {@code localhost:9092} and {@code http://localhost:8081}.
 * Override via env {@code KAFKA_BOOTSTRAP_SERVERS} / {@code SCHEMA_REGISTRY_URL}
 * or system properties {@code -Dkafka.bootstrap} / {@code -Dschema.registry.url}.</p>
 *
 * <p>The events are Avro-encoded with the same {@link KafkaAvroSerializer} the
 * producers use, so the schemas are registered in Schema Registry automatically —
 * exactly what the service's consumer expects.</p>
 */
public final class SmokeEventPublisher {

    private SmokeEventPublisher() {
    }

    public static void main(String[] args) {
        String bootstrap = setting("kafka.bootstrap", "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String schemaRegistry = setting("schema.registry.url", "SCHEMA_REGISTRY_URL", "http://localhost:8081");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry);

        System.out.printf("publishing sample events to %s (schema registry %s)%n", bootstrap, schemaRegistry);

        long nowNs = System.currentTimeMillis() * 1_000_000L;
        try (Producer<String, Object> producer = new KafkaProducer<>(props)) {
            send(producer, "accounting.trade", "T-1", tradeSettled(nowNs));
            // Cash movements are carried by the double-entry ledger (asset CASH):
            // a trade fee leg, a deposit and a withdrawal.
            send(producer, "accounting.ledger", "E-1", cashEntry(1L, 1001L, "0", "150.00", "T-1", "BUY", nowNs));
            send(producer, "accounting.ledger", "E-2", cashEntry(2L, 1001L, "0", "10000.00", "DEP-1", "DEPOSIT", nowNs));
            send(producer, "accounting.ledger", "E-3", cashEntry(3L, 1001L, "2500.00", "0", "WDR-1", "WITHDRAWAL", nowNs));
            producer.flush();
        }

        System.out.println("done - check http://localhost:8086/accounting/summary "
                + "and http://localhost:8086/accounting/trade-counts");
    }

    private static void send(Producer<String, Object> producer, String topic, String key, Object event) {
        producer.send(new ProducerRecord<>(topic, key, event), (metadata, error) -> {
            if (error != null) {
                System.err.printf("FAILED  %-22s : %s%n", topic, error.getMessage());
            } else {
                System.out.printf("sent    %-22s -> partition %d offset %d%n",
                        topic, metadata.partition(), metadata.offset());
            }
        });
    }

    private static TradeSettledEvent tradeSettled(long timestampNs) {
        return TradeSettledEvent.newBuilder()
                .setTradeId(1L)
                .setSymbol("AAPL")
                .setPrice("150.00")
                .setQuantity("10")
                .setBuyerFee("1.50")
                .setSellerFee("1.50")
                .setMakerOrderId(100L)
                .setTakerOrderId(101L)
                .setMakerAccountId(1001L)
                .setTakerAccountId(1002L)
                .setBuyerAccountId(1001L)
                .setSellerAccountId(1002L)
                .setTimestampNs(timestampNs)
                .build();
    }

    private static LedgerEntryRecordedEvent cashEntry(long entryId, long accountId, String debit,
                                                      String credit, String reference, String reason,
                                                      long timestampNs) {
        return LedgerEntryRecordedEvent.newBuilder()
                .setEntryId(entryId)
                .setAccountId(accountId)
                .setAsset("CASH")
                .setDebit(debit)
                .setCredit(credit)
                .setReference(reference)
                .setReason(reason)
                .setTimestampNs(timestampNs)
                .build();
    }

    private static String setting(String systemProperty, String envVar, String defaultValue) {
        String value = System.getProperty(systemProperty);
        if (value == null || value.isBlank()) {
            value = System.getenv(envVar);
        }
        return (value == null || value.isBlank()) ? defaultValue : value;
    }
}
