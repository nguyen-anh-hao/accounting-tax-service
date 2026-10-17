package com.exchange.accounting.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * OpenAPI (Swagger) metadata. The interactive UI is served at
 * {@code /swagger-ui.html} and the raw spec at {@code /v3/api-docs}.
 */
@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI accountingOpenApi() {
        return new OpenAPI().info(new Info()
                .title("Accounting & Tax API")
                .version("1.0.0")
                .description("""
                        Read-only accounting & tax read-model for the exchange.

                        The service consumes settlement events from the `accounting.*` Kafka
                        topics (relayed via the clearing-settlement transactional outbox +
                        Debezium CDC) and derives fee revenue, notional, cash-flow and tax
                        liability. It does not own the ledger; every endpoint here is a query.

                        Monetary values are exchanged as plain fixed-point strings to preserve
                        precision across languages."""));
    }
}
