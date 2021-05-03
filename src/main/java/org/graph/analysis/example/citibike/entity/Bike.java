package org.graph.analysis.example.citibike.entity;

import org.graph.analysis.entity.Vertex;

public class Bike extends Vertex {
    public Bike(String id) {
        super(id, VertexLabel.Bike.getLabel());
    }
}
