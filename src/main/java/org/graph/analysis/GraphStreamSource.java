package org.graph.analysis;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.graph.analysis.entity.ControlMessage;
import org.graph.analysis.entity.Edge;
import org.graph.analysis.entity.Vertex;
import org.graph.analysis.hash.HashPartition;
import org.graph.analysis.operator.StreamToGraph;
import org.graph.analysis.utils.BroadcastStreamUtil;
import org.graph.analysis.utils.PropertiesUtil;

import java.io.Serializable;
import java.util.Properties;

public class GraphStreamSource implements Serializable {
    private static final long serialVersionUID = 1L;
    public static String defaultWindowSize = "30.seconds";
    public static String defaultSlideSize = "10.seconds";
    public transient StreamTableEnvironment tableEnvironment;
    public transient BroadcastStream<ControlMessage> controlSignalStream;
    public transient GraphStream graphDataStream;
    public transient StreamExecutionEnvironment environment;
    public ControlMessage controlMessage = ControlMessage.buildDefault(defaultWindowSize, defaultSlideSize);

    public GraphStream fromKafka(String groupId, String topic, StreamToGraph<String> stringStreamToGraph) {
        Properties properties = PropertiesUtil.getProperties(groupId);

        final MapStateDescriptor<String, ControlMessage> controlMessageDescriptor = ControlMessage.getDescriptor();

        environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        tableEnvironment = StreamTableEnvironment.create(environment);


        // register kafka as consumer to consume topic
        DataStream<String> stringDataStream = environment.addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties));

        DataStream<Edge<Vertex, Vertex>> dataStream = stringDataStream
                .flatMap(stringStreamToGraph)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Edge<Vertex, Vertex>>() {
                    @Override
                    public long extractAscendingTimestamp(Edge<Vertex, Vertex> element) {
                        return element.getTimestamp();
                    }

                })
                .partitionCustom(new HashPartition(), "id");
        // register kafka as consumer to consume topic: control as broadcast stream
        controlSignalStream = BroadcastStreamUtil.getControlMessageBroadcastStream(properties, controlMessageDescriptor, environment);
        graphDataStream = new GraphStream(dataStream, this.getKeySelector());

        KeyedStream<Edge<Vertex, Vertex>, String> edgeStringKeyedStream = graphDataStream
                .connect(controlSignalStream)
                .process(new KeyedBroadcastProcessFunction<String, Edge<Vertex, Vertex>, ControlMessage, Edge<Vertex, Vertex>>() {
                    @Override
                    public void processElement(Edge<Vertex, Vertex> value, ReadOnlyContext ctx, Collector<Edge<Vertex, Vertex>> out) throws Exception {
                        out.collect(value);
                    }


                    @Override
                    public void processBroadcastElement(ControlMessage value, Context
                            ctx, Collector<Edge<Vertex, Vertex>> out) throws Exception {
                        BroadcastState<String, ControlMessage> controlMessageBroadcastState = ctx.getBroadcastState(ControlMessage.getDescriptor());

                        // update the state value using new state value from broadcast stream
                        controlMessageBroadcastState.put("control", value);
                        controlMessage.copy(value);

                    }
                })
                .keyBy(getKeySelector());

        // init graphStream by keyed stream
        graphDataStream = new GraphStream(edgeStringKeyedStream, edgeStringKeyedStream.getKeySelector());
        graphDataStream.setControlMessage(controlMessage);

        return graphDataStream;
    }

    private KeySelector<Edge<Vertex, Vertex>, String> getKeySelector() {
        return new KeySelector<Edge<Vertex, Vertex>, String>() {
            @Override
            public String getKey(Edge<Vertex, Vertex> value) throws Exception {
                return value.getLabel();
            }
        };
    }

    public StreamTableEnvironment getTableEnvironment() {
        return tableEnvironment;
    }

    public BroadcastStream<ControlMessage> getControlSignalStream() {
        return controlSignalStream;
    }

    public GraphStream getGraphDataStream() {
        return graphDataStream;
    }

    public StreamExecutionEnvironment getEnvironment() {
        return environment;
    }
}
