package org.graph.analysis.operator;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.graph.analysis.GraphStream;
import org.graph.analysis.entity.Edge;
import org.graph.analysis.entity.GraphContainer;
import org.graph.analysis.entity.Vertex;
import org.graph.analysis.network.Server;

import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class MyDataSink implements SinkFunction<Edge<Vertex, Vertex>> {
    protected int threshold = 20;
    protected GraphContainer graphContainer = new GraphContainer();
    protected Long lastSunkAt = new Date().getTime();
    protected Long slideSize;
    protected boolean isGroup;
    private GraphStream graphStream;

    public MyDataSink(GraphStream graphStream) {
        this.graphStream = graphStream;
        this.slideSize = getSlideSize(graphStream.getControlMessage().getSlideSize());
        this.isGroup = graphStream.getControlMessage().isWithGrouping();

    }


    @Override
    public void invoke(Edge<Vertex, Vertex> value, Context context) throws Exception {
        graphContainer.setEdges(graphStream.getEdges());
        graphContainer.setVertices(graphStream.getVertices());

        long timestamp = new Timestamp(System.currentTimeMillis()).getTime();
        threshold = 100;
        send(timestamp);
    }

    protected void send(long timestamp) {
        if (lastSunkAt + slideSize <= timestamp) {
            Server.sendToAll(graphContainer.toString());
            graphContainer.clear();
            lastSunkAt = timestamp;
            return;
        }

        if (graphContainer.size() >= threshold) {
            Server.sendToAll(graphContainer.toString());
            graphContainer.clear();
        }
    }

    private Long getSlideSize(String slideSize) {
        String[] s = slideSize.split("\\.");
        long sSize = Long.parseLong(s[0]);
        String sUnit = s[1].toUpperCase();
        return Time.of(sSize, TimeUnit.valueOf(sUnit)).toMilliseconds();
    }
}

