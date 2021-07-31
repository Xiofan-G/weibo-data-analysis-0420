package org.graph.analysis.entity;

import com.alibaba.fastjson.JSON;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.HashMap;

public class Edge<S, T> implements Serializable {

    public static final long serialVersionUID = 1L;
    public String id;
    public String label;
    public S source;
    public T target;
    public String remark;
    public Long timestamp;
    public Integer count;
    public String properties;
    public ControlMessage controlMessage;

    public Edge() {
    }

    public Edge(S source, T target, String label, String id, Long timestamp, HashMap<String, String> properties) {
        this.id = id;
        this.label = label;
        this.source = source;
        this.target = target;
        this.timestamp = timestamp;
        this.count = 1;
        if (properties.size() > 0) {
            this.properties = JSON.toJSONString(properties);
        } else {
            this.properties = "{}";
        }
    }

    public static Edge<Vertex, Vertex> of(Object sourceLabel, Object targetLabel, Object edgeLabel, Object count) {
        Vertex source = Edge.addVertex(sourceLabel, sourceLabel, (int) count);
        Vertex target = Edge.addVertex(targetLabel, targetLabel, (int) count);
        return new Edge<>(source, target, edgeLabel.toString(), edgeLabel.toString(), new Timestamp(System.currentTimeMillis()).getTime(), new HashMap<>());
    }

    public static Vertex addVertex(Object id, Object type, int count) {
        Vertex vertex = new Vertex(id.toString(), type.toString());
        vertex.addCount(count);
        return vertex;
    }

    public Integer getCount() {
        return count;
    }

    public ControlMessage getControlMessage() {
        return this.controlMessage;
    }

    public void setControlMessage(ControlMessage controlMessage) {
        this.controlMessage = controlMessage;
    }

    @Override
    public String toString() {
        String source = "";
        String target = "";
        if (this.source instanceof Vertex) {
            source = ((Vertex) this.source).getId();
            target = ((Vertex) this.target).getId();
        } else {
            source = this.source.toString();
            target = this.target.toString();
        }

        return String.format("{" +
                        "\"id\":\"%s\"," +
                        "\"label\":\"%s\"," +
                        "\"count\":%d," +
                        "\"source\":\"%s\"," +
                        "\"target\":\"%s\"," +
                        "\"timestamp\":%d," +
                        "\"properties\":%s" +
                        "}",
                id, label, count, source, target, timestamp, properties);
    }


    public Long getTimestamp() {
        return this.timestamp;
    }

    public String getLabel() {
        return this.label;
    }

    public void setLabel(String label) {
        this.label = label;

    }

    public String getId() {
        return this.id;
    }

    public S getSource() {
        return this.source;
    }

    public T getTarget() {
        return target;
    }

    public String getRemark() {
        return this.remark;
    }

    public void addCount(int count) {
        this.count += count;
    }


}