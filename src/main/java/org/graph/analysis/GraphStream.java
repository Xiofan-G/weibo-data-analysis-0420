package org.graph.analysis;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.graph.analysis.entity.ControlMessage;
import org.graph.analysis.entity.Edge;
import org.graph.analysis.entity.Vertex;
import org.graph.analysis.operator.GraphApply;

public class GraphStream extends DataStream<Edge<Vertex, Vertex>> {
    private static final long serialVersionUID = 1L;
    public ControlMessage controlMessage;


    public GraphStream(StreamExecutionEnvironment environment, StreamTransformation<Edge<Vertex, Vertex>> transformation) {
        super(environment, transformation);
    }

    public GraphStream apply(GraphApply<GraphStream> apply) {
        return apply.run(this);
    }
}
