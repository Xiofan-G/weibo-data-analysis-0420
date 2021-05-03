package org.graph.analysis.example.citibike.entity;

import org.graph.analysis.entity.Vertex;

public class Station extends Vertex {
    public String latitude;
    public String longitude;
    public String name;

    public Station(String id, String name, String latitude, String longitude) {
        super(id, VertexLabel.Station.getLabel());
        this.latitude = latitude;
        this.longitude = longitude;
        this.name = name;
    }

    @Override
    public String toString() {
        String properties = String.format(
                "\"properties\":" +
                        "{" +
                        "\"name\":\"%s\"," +
                        "\"latitude\":\"%s\"," +
                        "\"longitude\":\"%s\"" +
                        "}",
                name, latitude, longitude);
        return String.format("%s,%s}", super.toString().replace("}", ""), properties);
    }

}
