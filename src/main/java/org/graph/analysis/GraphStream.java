package org.graph.analysis;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.graph.analysis.entity.ControlMessage;
import org.graph.analysis.entity.Edge;
import org.graph.analysis.entity.Vertex;
import org.graph.analysis.operator.GraphApply;

public class GraphStream extends DataStream<Edge<Vertex, Vertex>> {
    //继承datastream为了继承filter等算子的功能
    private static final long serialVersionUID = 1L;
    public ControlMessage controlMessage;


    public GraphStream(StreamExecutionEnvironment environment, StreamTransformation<Edge<Vertex, Vertex>> transformation) {
        super(environment, transformation);
    }

    public GraphStream apply(GraphApply<GraphStream> apply) {
        //基于graphapply接口，传入的实现这个接口的一个对象
        //直接调用传入的这个对象里面的run方法
        return apply.run(this);
    }
}
