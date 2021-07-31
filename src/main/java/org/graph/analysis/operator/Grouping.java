package org.graph.analysis.operator;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.graph.analysis.GraphStream;
import org.graph.analysis.entity.ControlMessage;
import org.graph.analysis.entity.Edge;
import org.graph.analysis.entity.Vertex;

public class Grouping extends KeyedProcessFunction<String, Edge<Vertex, Vertex>, Edge<Vertex, Vertex>> implements GraphApply<GraphStream> {
    /**
     * Use Flink state to hold the grouping information from broadcast stream
     */
    private transient ValueState<Boolean> withGroupingState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Boolean> withGroupingStateDescriptor = new ValueStateDescriptor<>(ControlMessage.withGroupStateName, BasicTypeInfo.BOOLEAN_TYPE_INFO, Boolean.FALSE);
        withGroupingState = getRuntimeContext().getState(withGroupingStateDescriptor);
    }

    @Override
    public GraphStream run(GraphStream graphStream) {
        DataStream<Edge<Vertex, Vertex>> edgeDataStream = graphStream.keyBy(new EdgeKeySelector()).process(this);
        return new GraphStream(edgeDataStream.getExecutionEnvironment(), edgeDataStream.getTransformation());
    }

    /**
     * Do grouping by label
     *
     * @param value date element from upper operator
     * @param ctx   Environment context
     * @param out   collector
     * @throws Exception default exception
     */
    @Override
    public void processElement(Edge<Vertex, Vertex> value, Context ctx, Collector<Edge<Vertex, Vertex>> out) throws Exception {
        this.updateWithGroupingState(value);

        if (this.withGroupingState.value()) {
            Edge<Vertex, Vertex> newEdge = Edge.of(value.getSource().getLabel(), value.getTarget().getLabel(), value.getLabel(), 1);
            newEdge.setControlMessage(value.getControlMessage());
            out.collect(newEdge);
            return;
        }
        out.collect(value);
    }

    /**
     * Update grouping from data's control message tag
     *
     * @param value data element
     * @throws Exception
     */
    private void updateWithGroupingState(Edge<Vertex, Vertex> value) throws Exception {
        ControlMessage controlMessage = value.getControlMessage();
        if (controlMessage == null) {
            return;
        }
        if (value.getControlMessage().getWithGrouping().equals(this.withGroupingState.value()))
            return;

        this.withGroupingState.update(controlMessage.getWithGrouping());
    }
}