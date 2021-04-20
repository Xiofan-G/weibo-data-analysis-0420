package org.graph.analysis;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.graph.analysis.entity.ControlMessage;
import org.graph.analysis.entity.Edge;
import org.graph.analysis.entity.Vertex;
import org.graph.analysis.operator.GraphApply;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.HashMap;

public class GraphStream extends KeyedStream<Edge<Vertex, Vertex>, String> implements Serializable {
    private static final long serialVersionUID = 1L;
    private HashMap<String, Edge<Vertex, Vertex>> edges;
    private HashMap<String, Vertex> vertices;
    private String version;
    private StreamTableEnvironment tableEnvironment;
    private BroadcastStream<ControlMessage> controlSignalStream;
    private DataStreamSource<Edge<Vertex, Vertex>> graphDataStream;
    private ControlMessage controlMessage;
    private boolean isGrouping;
    private Table groupedTable;


    public GraphStream(DataStream<Edge<Vertex, Vertex>> dataStream, KeySelector<Edge<Vertex, Vertex>, String> keySelector, HashMap<String, Edge<Vertex, Vertex>> edges, HashMap<String, Vertex> vertices) {
        super(dataStream, keySelector);
        this.edges = edges;
        this.vertices = vertices;
    }


    public GraphStream apply(GraphApply<GraphStream> apply) {
        apply.run(this);
        return this;
    }

    public HashMap<String, String> getVertexFilter() {
        return this.getFilter(controlMessage.getVertexLabel());
    }

    public HashMap<String, String> getEdgeFilter() {
        return this.getFilter(controlMessage.getEdgeLabel());
    }

    private HashMap<String, String> getFilter(String labelString) {
        HashMap<String, String> filter = new HashMap<>();
        if (labelString == null || "".equals(labelString)) {
            return filter;
        }

        String[] vertexLabels = controlMessage.getVertexLabel().split(",");
        for (String label : vertexLabels) {
            filter.put(label, label);
        }
        return filter;
    }

    public HashMap<String, Edge<Vertex, Vertex>> getEdges() {
        return edges;
    }

    public void setEdges(HashMap<String, Edge<Vertex, Vertex>> edges) {
        this.edges = edges;
    }

    public HashMap<String, Vertex> getVertices() {
        return vertices;
    }

    public void setVertices(HashMap<String, Vertex> vertices) {
        this.vertices = vertices;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public StreamTableEnvironment getTableEnvironment() {
        return tableEnvironment;
    }

    public void setTableEnvironment(StreamTableEnvironment tableEnvironment) {
        this.tableEnvironment = tableEnvironment;
    }

    public BroadcastStream<ControlMessage> getControlSignalStream() {
        return controlSignalStream;
    }

    public void setControlSignalStream(BroadcastStream<ControlMessage> controlSignalStream) {
        this.controlSignalStream = controlSignalStream;
    }

    public DataStreamSource<Edge<Vertex, Vertex>> getGraphDataStream() {
        return graphDataStream;
    }

    public void setGraphDataStream(DataStreamSource<Edge<Vertex, Vertex>> graphDataStream) {
        this.graphDataStream = graphDataStream;
    }

    public ControlMessage getControlMessage() {
        return controlMessage;
    }

    public void setControlMessage(ControlMessage controlMessage) {
        this.controlMessage = controlMessage;
    }

    public boolean isGrouping() {
        return isGrouping;
    }

    public void setGrouping(boolean grouping) {
        isGrouping = grouping;
    }

    public Table getGroupedTable() {
        return groupedTable;
    }

    public void setGroupedTable(Table groupedTable) {
        this.groupedTable = groupedTable;
    }

    /**
     * Add grouped edge and vertex
     *
     * @param sourceLabel
     * @param targetLabel
     * @param edgeLabel
     * @param count
     */
    public Edge<Vertex, Vertex> addEdge(Object sourceLabel, Object targetLabel, Object edgeLabel, Object count) {
        Vertex source = this.addVertex(sourceLabel, sourceLabel, (int) count);
        Vertex target = this.addVertex(targetLabel, targetLabel, (int) count);

        Edge<Vertex, Vertex> edge = new Edge<>(source, target, edgeLabel.toString(), edgeLabel.toString(), new Timestamp(System.currentTimeMillis()).getTime(), new HashMap<>());
        edge.addCount((int) count);
        edges.put(edgeLabel.toString(), edge);
        return edge;

    }

    public Vertex addVertex(Object id, Object type, int count) {
        Vertex vertex = this.vertices.get(id.toString());
        if (this.vertices.get(id.toString()) == null) {
            vertex = new Vertex(id.toString(), type.toString());
            this.vertices.put(id.toString(), vertex);
        }
        vertex.addCount(count);
        return vertex;
    }

}
