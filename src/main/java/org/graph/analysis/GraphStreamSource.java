package org.graph.analysis;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.graph.analysis.entity.ControlMessage;
import org.graph.analysis.entity.Edge;
import org.graph.analysis.entity.Vertex;
import org.graph.analysis.hash.HashPartition;
import org.graph.analysis.operator.DynamicSlideEventTimeWindow;
import org.graph.analysis.operator.StreamToGraph;
import org.graph.analysis.utils.BroadcastStreamUtil;
import org.graph.analysis.utils.PropertiesUtil;

import java.io.Serializable;
import java.util.Properties;

public class GraphStreamSource implements Serializable {
    private static final long serialVersionUID = 1L;
    public transient BroadcastStream<ControlMessage> controlSignalStream;
    public transient StreamExecutionEnvironment environment;

    public GraphStream fromKafka(String groupId, String topic, StreamToGraph<String> stringStreamToGraph) {
        Properties properties = PropertiesUtil.getProperties(groupId);

        final MapStateDescriptor<String, ControlMessage> controlMessageDescriptor = ControlMessage.getDescriptor();

        environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // register kafka as consumer to consume topic
        DataStream<String> stringDataStream = environment.addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties));
        DataStream<Edge<Vertex, Vertex>> dataStream = getEdgeStreamFromString(stringStreamToGraph, stringDataStream);

        // register kafka as consumer to consume topic: control as broadcast stream
        controlSignalStream = BroadcastStreamUtil.fromKafka(properties, controlMessageDescriptor, environment);


        return getGraphStreamByConnectingBroadcast(dataStream);
    }

    public GraphStream fromSocket(StreamToGraph<String> stringStreamToGraph) {

        final MapStateDescriptor<String, ControlMessage> controlMessageDescriptor = ControlMessage.getDescriptor();

        environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // register kafka as consumer to consume topic
        DataStream<String> stringDataStream = environment.socketTextStream("localhost", 10000);
        DataStream<Edge<Vertex, Vertex>> dataStream = getEdgeStreamFromString(stringStreamToGraph, stringDataStream);

        // register kafka as consumer to consume topic: control as broadcast stream
        controlSignalStream = BroadcastStreamUtil.fromSocket(controlMessageDescriptor, environment);

        return getGraphStreamByConnectingBroadcast(dataStream);
    }

    private DataStream<Edge<Vertex, Vertex>> getEdgeStreamFromString(StreamToGraph<String> stringStreamToGraph, DataStream<String> stringDataStream) {
        return stringDataStream
                .flatMap(stringStreamToGraph)
                .partitionCustom(new HashPartition(), "id");
    }

    /**
     * Mixin Broadcast with data stream
     *
     * @param dataStream data stream
     * @return
     */
    private GraphStream getGraphStreamByConnectingBroadcast(DataStream<Edge<Vertex, Vertex>> dataStream) {
        DataStream<Edge<Vertex, Vertex>> edgeDataStream = dataStream
                .connect(controlSignalStream)
                .process(new BroadcastProcessFunction<Edge<Vertex, Vertex>, ControlMessage, Edge<Vertex, Vertex>>() {
                    @Override
                    public void processElement(Edge<Vertex, Vertex> value, ReadOnlyContext ctx, Collector<Edge<Vertex, Vertex>> out) throws Exception {
                        ReadOnlyBroadcastState<String, ControlMessage> controlMessageBroadcastState = ctx.getBroadcastState(ControlMessage.getDescriptor());

                        // update the state value using new state value from broadcast stream
                        ControlMessage controlMessage = controlMessageBroadcastState.get(ControlMessage.controlLabel);

                        // mixin the control message to data stream, just for transfer data to follow operator
                        if (controlMessage != null) {
                            value.setControlMessage(controlMessage);
                        }
                        out.collect(value);
                    }

                    @Override
                    public void processBroadcastElement(ControlMessage value, Context
                            ctx, Collector<Edge<Vertex, Vertex>> out) throws Exception {
                        BroadcastState<String, ControlMessage> controlMessageBroadcastState = ctx.getBroadcastState(ControlMessage.getDescriptor());
                        // update the state value using new state value from broadcast stream
                        controlMessageBroadcastState.put(ControlMessage.controlLabel, value);
                        System.out.println("process broadcast control message: " + value.toString());

                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Edge<Vertex, Vertex>>() {

                    @Override
                    public long extractAscendingTimestamp(Edge<Vertex, Vertex> element) {
                        return element.getTimestamp();
                    }
                })
                .windowAll(DynamicSlideEventTimeWindow.of(ControlMessage.getDefaultWindowSize(), ControlMessage.getDefaultSlideSize()))
                .process(new ProcessAllWindowFunction<Edge<Vertex, Vertex>, Edge<Vertex, Vertex>, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Edge<Vertex, Vertex>> elements, Collector<Edge<Vertex, Vertex>> out) throws Exception {
                        for (Edge<Vertex, Vertex> edge : elements) {
                            out.collect(edge);
                        }

                    }
                });

        return new GraphStream(edgeDataStream.getExecutionEnvironment(), edgeDataStream.getTransformation());
    }

    public StreamExecutionEnvironment getEnvironment() {
        return environment;
    }
}
