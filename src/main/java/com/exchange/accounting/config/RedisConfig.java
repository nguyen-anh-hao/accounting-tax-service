package com.exchange.accounting.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

/**
 * Redis is the accounting read-model store. It is configured from the same
 * REDIS_ADDR ("host:port") used by the other services so accounting replicas
 * share one aggregate view and keep no state in process memory.
 */
@Configuration
public class RedisConfig {

    @Bean
    public RedisConnectionFactory redisConnectionFactory(
            @Value("${REDIS_ADDR:localhost:6379}") String addr) {
        String host = addr;
        int port = 6379;
        int sep = addr.lastIndexOf(':');
        if (sep > 0) {
            host = addr.substring(0, sep);
            port = Integer.parseInt(addr.substring(sep + 1));
        }
        return new LettuceConnectionFactory(new RedisStandaloneConfiguration(host, port));
    }
}
