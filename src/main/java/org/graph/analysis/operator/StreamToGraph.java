package org.graph.analysis.operator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.graph.analysis.entity.Edge;
import org.graph.analysis.entity.Vertex;

public interface StreamToGraph<T> extends FlatMapFunction<T, Edge<Vertex, Vertex>> {

    @Override
    void flatMap(T value, Collector<Edge<Vertex, Vertex>> out) throws Exception;
}
