package org.graph.analysis.utils;

import java.util.Properties;

public class PropertiesUtil {
    private static final String kafkaHost = "172.31.132.5:9092";
    private static final String zookeeperHost = "localhost:2181";

    public static Properties getProperties(String groupId) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaHost);
        properties.setProperty("zookeeper.connect", zookeeperHost);
        properties.setProperty("group.id", groupId);
        return properties;
    }
}
