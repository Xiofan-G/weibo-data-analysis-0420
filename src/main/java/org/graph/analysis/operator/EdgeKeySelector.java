package org.graph.analysis.operator;

import org.apache.flink.api.java.functions.KeySelector;
import org.graph.analysis.entity.Edge;
import org.graph.analysis.entity.Vertex;

/**
 * Key selector to fit the every keyed operator
 */
public class EdgeKeySelector implements KeySelector<Edge<Vertex, Vertex>, String> {
    @Override
    public String getKey(Edge<Vertex, Vertex> value) throws Exception {
        return value.getLabel();
    }

}
