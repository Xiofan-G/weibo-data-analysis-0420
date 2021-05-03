package org.graph.analysis.example.weibo.operator;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.flink.util.Collector;
import org.graph.analysis.example.weibo.entity.*;
import org.graph.analysis.entity.Edge;
import org.graph.analysis.operator.StreamToGraph;
import org.graph.analysis.entity.Vertex;

import java.util.HashMap;

public class WeiboDataToEdge implements StreamToGraph<String> {
    private Edge<Vertex, Vertex> from(Edge<String, String> edge) {
        RelationLabel label = RelationLabel.valueOf(edge.getLabel());

        String source = edge.getSource();
        String target = edge.getTarget();
        String remark = edge.getRemark();
        Vertex sourceVertex;
        Vertex targetVertex;
        switch (label) {
            case Fans:
                sourceVertex = JSON.parseObject(source, User.class);
                targetVertex = JSON.parseObject(target, User.class);
                break;
            case At:

                targetVertex = JSON.parseObject(target, User.class);
                if (VertexLabel.Weibo.name().equals(remark)) {
                    sourceVertex = JSON.parseObject(source, Weibo.class);

                } else {
                    sourceVertex = JSON.parseObject(source, Comment.class);
                }

                break;
            case Author:
                sourceVertex = JSON.parseObject(source, User.class);
                if (VertexLabel.Weibo.name().equals(remark)) {
                    targetVertex = JSON.parseObject(target, Weibo.class);

                } else {
                    targetVertex = JSON.parseObject(target, Comment.class);
                }
                break;
            case ReplyOf:
                sourceVertex = JSON.parseObject(source, Comment.class);
                if (VertexLabel.Weibo.name().equals(remark)) {
                    targetVertex = JSON.parseObject(target, Weibo.class);

                } else {
                    targetVertex = JSON.parseObject(target, Comment.class);
                }
                break;
            case BelongTo:
                sourceVertex = JSON.parseObject(source, Weibo.class);
                targetVertex = JSON.parseObject(target, Tag.class);
                break;
            case Mentioned:
                targetVertex = JSON.parseObject(target, Weibo.class);
                if (VertexLabel.Weibo.name().equals(remark)) {
                    sourceVertex = JSON.parseObject(source, Weibo.class);

                } else {
                    sourceVertex = JSON.parseObject(source, Comment.class);
                }
                break;
            default:
                sourceVertex = JSON.parseObject(source, Vertex.class);
                targetVertex = JSON.parseObject(target, Vertex.class);

        }
        HashMap<String, String> properties = new HashMap<>();
        properties.put("remark", edge.getRemark());
        return new Edge<>(sourceVertex, targetVertex, label.name(), edge.getId(), edge.getTimestamp(), properties);
    }

    @Override
    public void flatMap(String value, Collector<Edge<Vertex, Vertex>> out) throws Exception {
        Edge<String, String> edge = JSON.parseObject(value, new TypeReference<Edge<String, String>>(Edge.class) {
        });
        out.collect(from(edge));
    }
}
