package org.graph.analysis.example.weibo.entity;

public enum RelationLabel {
    Author,
    Fans,
    BelongTo,
    At,
    Mentioned,
    ReplyOf;

    public String getLabel() {
        return this.name();
    }
}
