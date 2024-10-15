package com.javarush.config;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisUtil {
    private static RedisClient redisClient;
                                                                                                                                                                                                                                                                private static final Logger logger = LoggerFactory.getLogger(RedisUtil.class);

    private static RedisClient prepareClient() {
        RedisClient redisClient = RedisClient.create(RedisURI.create("localhost", 6379));
        try (StatefulRedisConnection<String, String> connection = redisClient.connect()) {
            logger.info("Connected to Redis");
        }
        return redisClient;
    }

    public static RedisClient getClient() {
        if (redisClient == null) {
            redisClient = prepareClient();
        }
        return redisClient;
    }
}