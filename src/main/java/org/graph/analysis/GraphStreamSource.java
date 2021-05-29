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
        //【1】
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
                //广播流的生命周期其实已经结束了
                .process(new BroadcastProcessFunction<Edge<Vertex, Vertex>, ControlMessage, Edge<Vertex, Vertex>>() {
                    @Override
                    public void processElement(Edge<Vertex, Vertex> value, ReadOnlyContext ctx, Collector<Edge<Vertex, Vertex>> out) throws Exception {
                        ReadOnlyBroadcastState<String, ControlMessage> controlMessageBroadcastState = ctx.getBroadcastState(ControlMessage.getDescriptor());

                        // update the state value using new state value from broadcast stream
                        ControlMessage controlMessage = controlMessageBroadcastState.get(ControlMessage.controlLabel);

                        // mixin the control message to data stream, just for transfer data to follow operator
                        if (controlMessage != null) {
                            value.setControlMessage(controlMessage);

                        } else {
                            value.setControlMessage(ControlMessage.buildDefault());
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
                //数据是基于event time的。一般的开窗：在合流之前设置了一个水印的提取器，把数据打上一个水印标识，然后再进行开窗，最后再进行广播流的合并。
                //（概念类似数据上的水印时间和窗口区间进行对比，如果超过了这个时间则进入下一个窗口）
                //先合并再进行开窗，之前的代码在【1】位置进行水印提取，但是提取无效，窗口无法触发。
                //猜测：之前只对了一个流设置了水印提取，（可能是因为合并之后产生的新流，水印被抹掉了？也可能是两个流本身水印不一样，只对一个进行提取，无效）
                // 但是合并之后没有水印提取的设定，所以系统不知道如何提取，导致数据一直在windows里面空转，他不会触发窗口计算。
                //合并完了之后再进行水印的提取，数据上面打了标识
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Edge<Vertex, Vertex>>() {//指派时间戳，并生成WaterMark 作用？

                    @Override
                    public long extractAscendingTimestamp(Edge<Vertex, Vertex> element) {
                        return element.getTimestamp();
                    }
                })
                //动态窗口的大小放到了数据上面，根据数据上的标识来动态分配窗口
                .windowAll(DynamicSlideEventTimeWindow.of(ControlMessage.getDefaultWindowSize(), ControlMessage.getDefaultSlideSize()))
                .process(new ProcessAllWindowFunction<Edge<Vertex, Vertex>, Edge<Vertex, Vertex>, TimeWindow>() {
                    //为了把allwindowedstream再次转换为datastream
                    @Override
                    public void process(Context context, Iterable<Edge<Vertex, Vertex>> elements, Collector<Edge<Vertex, Vertex>> out) throws Exception {
                        for (Edge<Vertex, Vertex> edge : elements) {
                            out.collect(edge);
                        }

                    }
                });
              //为了返回自定义的graphstream，从process中返回的stream中获得执行环境和执行结果
        return new GraphStream(edgeDataStream.getExecutionEnvironment(), edgeDataStream.getTransformation());
    }

    public StreamExecutionEnvironment getEnvironment() {
        return environment;
    }
}
