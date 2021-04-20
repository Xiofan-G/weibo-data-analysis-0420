package org.graph.analysis.entity;

import java.io.Serializable;
import java.util.HashMap;

public class Edge<S, T> implements Serializable {

    private static final long serialVersionUID = 1L;
    public String id;
    public String label;
    public S source;
    public T target;
    public String remark;
    public Long timestamp;
    public Integer count;
    public HashMap<String, String> properties;


    public Edge(S source, T target, String label, String id, Long timestamp, HashMap<String, String> properties) {
        this.id = id;
        this.label = label;
        this.source = source;
        this.target = target;
        this.timestamp = timestamp;
        this.count = 1;
        this.properties = properties;
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

        StringBuilder sb = new StringBuilder();
        sb.append("\"properties\":{");
        for (String key : properties.keySet()) {
            sb.append(String.format("\"%s\":\"%s\",", key, properties.get(key)));
        }
        sb.delete(sb.length() - 2, sb.length() - 1);
        sb.append("}");

        return String.format("{" +
                        "\"id\":\"%s\"," +
                        "\"label\":\"%s\"," +
                        "\"count\":%d," +
                        "\"source\":\"%s\"," +
                        "\"target\":\"%s\"," +
                        "\"timestamp\":%d," +
                        "%s" +
                        "}",
                id, label, count, source, target, timestamp, sb.toString());
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