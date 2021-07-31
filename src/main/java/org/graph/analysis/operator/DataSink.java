package org.graph.analysis.operator;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.graph.analysis.GraphStream;
import org.graph.analysis.entity.ControlMessage;
import org.graph.analysis.entity.Edge;
import org.graph.analysis.entity.GraphContainer;
import org.graph.analysis.entity.Vertex;
import org.graph.analysis.network.Server;

public class DataSink extends RichMapFunction<Edge<Vertex, Vertex>, Edge<Vertex, Vertex>> implements GraphApply<GraphStream> {
    private transient ValueState<Boolean> withGroupingState;

    private transient GraphContainer graphContainer;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Boolean> withGroupingStateDescriptor = new ValueStateDescriptor<>(ControlMessage.withGroupStateName, BasicTypeInfo.BOOLEAN_TYPE_INFO, Boolean.FALSE);
        withGroupingState = getRuntimeContext().getState(withGroupingStateDescriptor);
        graphContainer = new GraphContainer();
    }

    @Override
    public Edge<Vertex, Vertex> map(Edge<Vertex, Vertex> value) throws Exception {
        this.updateWithGroupingState(value);

        if (this.withGroupingState.value()) {
            graphContainer.addEdge(value.getSource().getLabel(), value.getTarget().getLabel(), value.getLabel(), value.getCount());
        } else {
            graphContainer.addEdge(value);
        }
        Server.sendToAll(graphContainer.toString());
        graphContainer.clear();
        return value;
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

    @Override
    public GraphStream run(GraphStream graphStream) {
        DataStream<Edge<Vertex, Vertex>> edgeDataStream = graphStream.keyBy(new EdgeKeySelector()).map(this);
        return new GraphStream(edgeDataStream.getExecutionEnvironment(), edgeDataStream.getTransformation());
    }
}
