package org.graph.analysis.operator;

import org.graph.analysis.GraphStream;
import org.graph.analysis.entity.Edge;
import org.graph.analysis.entity.Vertex;

import java.util.HashMap;
import java.util.Map;

public class SubGraphApply implements GraphApply<GraphStream> {
    public final Map<String, String> vertexFilter;
    public final Map<String, String> edgeFilter;

    public SubGraphApply(Map<String, String> vertexFilter, Map<String, String> edgeFilter) {
        this.vertexFilter = vertexFilter;
        this.edgeFilter = edgeFilter;
    }

    @Override
    public GraphStream run(GraphStream graphStream) {
        HashMap<String, Edge<Vertex, Vertex>> edges = graphStream.getEdges();
        HashMap<String, Vertex> vertices = graphStream.getVertices();
        this.filterEdge(vertices, edges);
        this.filterVertex(vertices, edges);
        return graphStream;
    }

    private void filterVertex(HashMap<String, Vertex> vertices, HashMap<String, Edge<Vertex, Vertex>> edges) {

        // if vertexFilter is null, it means do not filter vertex
        if (this.vertexFilter == null || this.vertexFilter.size() == 0) {
            return;
        }
        edges.entrySet().removeIf(entry -> !this.vertexFilter.containsKey(entry.getValue().getSource().getLabel())
                && !this.vertexFilter.containsKey(entry.getValue().getTarget().getLabel()));
        vertices.entrySet().removeIf(entry -> !this.vertexFilter.containsKey(entry.getValue().getLabel()));
    }

    private void filterEdge(HashMap<String, Vertex> vertices, HashMap<String, Edge<Vertex, Vertex>> edges) {
        // if edgeFilter is null, it means do not filter edge
        if (this.edgeFilter == null || this.edgeFilter.size() == 0) {
            return;
        }
        edges.entrySet().removeIf(entry -> !this.edgeFilter.containsKey(entry.getValue().getLabel()));
    }
}


