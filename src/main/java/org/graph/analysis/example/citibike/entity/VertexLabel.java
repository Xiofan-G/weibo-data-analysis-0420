package org.graph.analysis.example.citibike.entity;

public enum VertexLabel {
    Bike,
    User,
    Station;

    public String getLabel() {
        return this.name();
    }
}
