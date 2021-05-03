package org.graph.analysis.example.citibike.entity;

public enum RelationLabel {
    Ride,
    TripFrom,
    TripTo,
    RiddenFrom,
    RiddenTo,
    Control;

    public String getLabel() {
        return this.name();
    }
}
