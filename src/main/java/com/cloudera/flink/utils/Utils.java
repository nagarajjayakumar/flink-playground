package com.cloudera.flink.utils;

import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Properties;

/**
 * Utility functions.
 */
public class Utils {

  //  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    public static final String KAFKA_PREFIX = "kafka.";

    public static Properties readKafkaProperties(ParameterTool params) {
        Properties properties = new Properties();
        for (String key : params.getProperties().stringPropertyNames()) {
            if (key.startsWith(KAFKA_PREFIX)) {
                properties.setProperty(key.substring(KAFKA_PREFIX.length()), params.get(key));
            }
        }

//        LOG.info("### Kafka parameters:");
//        for (String key : properties.stringPropertyNames()) {
//            LOG.info("Kafka param: {}={}", key, properties.get(key));
//        }
        return properties;
    }

    private Utils() {
        throw new UnsupportedOperationException("Utils should not be instantiated!");
    }
}
