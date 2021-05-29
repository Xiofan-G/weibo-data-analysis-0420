package org.graph.analysis.operator;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.graph.analysis.GraphStream;
import org.graph.analysis.entity.ControlMessage;
import org.graph.analysis.entity.Edge;
import org.graph.analysis.entity.Vertex;

public class SubGraph extends RichFilterFunction<Edge<Vertex, Vertex>> implements GraphApply<GraphStream> {
    public transient MapState<String, String> edgeFilter;
    public transient MapState<String, String> vertexFilter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MapStateDescriptor<String, String> edgeFilterDescriptor = new MapStateDescriptor<>(
                ControlMessage.edgeFilterStateName,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO);

        MapStateDescriptor<String, String> vertexFilterDescriptor = new MapStateDescriptor<>(
                ControlMessage.vertexFilterStateName,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO);

        vertexFilter = getRuntimeContext().getMapState(vertexFilterDescriptor);
        edgeFilter = getRuntimeContext().getMapState(edgeFilterDescriptor);
    }


    /**
     * Fit RichFilterFunction to use chain programing
     *
     * @param value Data
     * @return need include or not
     * @throws Exception
     */
    @Override
    public boolean filter(Edge<Vertex, Vertex> value) throws Exception {
        // update filter state by data's tag
        this.updateFilterState(value);

        // if vertexFilter is null, it means do not filter vertex
        if (!this.vertexFilter.keys().iterator().hasNext()
                && !this.edgeFilter.keys().iterator().hasNext()) {
            return true;
        }
        boolean result = this.filterVertex(value) && this.filterEdge(value);
        if (!result) {
            System.out.println("Exclude: " + value.toString());
        }
        return result;
    }

    @Override
    public GraphStream run(GraphStream graphStream) {
        DataStream<Edge<Vertex, Vertex>> edgeDataStream = graphStream.keyBy(new EdgeKeySelector()).filter(this);
        return new GraphStream(edgeDataStream.getExecutionEnvironment(), edgeDataStream.getTransformation());
    }

    /**
     * Update Vertex filter and Edge filter by data's tag
     *
     * @param value data element
     * @throws Exception
     */
    private void updateFilterState(Edge<Vertex, Vertex> value) throws Exception {

        this.edgeFilter.clear();
        this.vertexFilter.clear();

        ControlMessage controlMessage = value.getControlMessage();
        if (controlMessage.getVertexLabel() != null
                && !"".equals(controlMessage.getVertexLabel())
        ) {
            for (String label : controlMessage.getVertexLabel().split(",")) {
                this.vertexFilter.put(label, label);
            }
        }

        if (controlMessage.getEdgeLabel() != null
                && !"".equals(controlMessage.getEdgeLabel())
        ) {
            for (String label : controlMessage.getEdgeLabel().split(",")) {
                this.edgeFilter.put(label, label);
            }
        }

    }

    /**
     * Filter data by vertex state
     *
     * @param value data element
     * @return true: include, false: exclude
     * @throws Exception
     */
    private boolean filterVertex(Edge<Vertex, Vertex> value) throws Exception {
        // if vertexFilter has no value, it means do not need filter vertex
        if (!this.vertexFilter.keys().iterator().hasNext()) {
            return true;
        }
        return this.vertexFilter.contains(value.getSource().getLabel())
                || this.vertexFilter.contains(value.getTarget().getLabel());
    }

    /**
     * Filter data by Edge state
     *
     * @param value date element
     * @return true: include, false: exclude
     * @throws Exception
     */
    private boolean filterEdge(Edge<Vertex, Vertex> value) throws Exception {
        // if edgeFilter has no value, it means do not need filter edge
        if (!this.edgeFilter.keys().iterator().hasNext()) {
            return true;
        }
        return this.edgeFilter.contains(value.getLabel());
    }


}


