package org.graph.analysis.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.graph.analysis.entity.ControlMessage;

import java.util.Properties;

public class BroadcastStreamUtil {
    public static BroadcastStream<ControlMessage> getControlMessageBroadcastStream(Properties properties, MapStateDescriptor<String, ControlMessage> controlMessageDescriptor, StreamExecutionEnvironment env) {
        final FlinkKafkaConsumer<String> kafkaControlSignalConsumer = new FlinkKafkaConsumer<>("control", new SimpleStringSchema(), properties);
        final DataStream<String> broadcastStream = env.addSource(kafkaControlSignalConsumer).setParallelism(1);
        return broadcastStream
                .map(new MapFunction<String, ControlMessage>() {
                    @Override
                    public ControlMessage map(String value) {
                        return ControlMessage.buildFromString(value);
                    }
                })
                .broadcast(controlMessageDescriptor);
    }
}
