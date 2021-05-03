package org.graph.analysis.operator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.Slide;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.graph.analysis.GraphStream;
import org.graph.analysis.entity.Edge;
import org.graph.analysis.entity.TupleEdge;
import org.graph.analysis.entity.Vertex;

public class GroupingApply implements GraphApply<GraphStream> {
    public boolean isGrouping = false;

    public GroupingApply(boolean isGrouping) {
        this.isGrouping = isGrouping;
    }

    @Override
    public GraphStream run(GraphStream graphStream) {

        if (!isGrouping) {
            return graphStream;
        }
        String windowSize = graphStream.getControlMessage().getWindowSize();
        if (windowSize.contains("milliseconds")) {
            windowSize = windowSize.substring(0, windowSize.length() - 6);
        }
        String slideSize = graphStream.getControlMessage().getSlideSize();
        if (slideSize.contains("milliseconds")) {
            slideSize = slideSize.substring(0, slideSize.length() - 6);
        }

        StreamTableEnvironment tableEnv = graphStream.getTableEnvironment();
        DataStream<TupleEdge> tupleEdgeDataStream = graphStream
                .map(new MapFunction<Edge<Vertex, Vertex>, TupleEdge>() {
                    @Override
                    public TupleEdge map(Edge<Vertex, Vertex> value) throws Exception {
                        return new TupleEdge(value);
                    }
                });
        Table table = tableEnv.fromDataStream(tupleEdgeDataStream, "f0, f1, f2, f3, f4, f5, f6.rowtime");


        Table edgeTable = table.window(Slide.over(windowSize).every(slideSize).on("f6")
                .as("statWindow"))
                .groupBy("statWindow, f1, f3, f5")
                .select("f3 as sourceLabel, f5 as targetLabel, f1 as edgeLabel , f0.count as edgeCount");


        Table sourceTable = table.select("f0,f2 as f1,f3 as f2,f6 as f3");

        Table targetTable = table.select("f0,f4 as f1,f5 as f2,f6 as f3");


        Table vertexTable = sourceTable.unionAll(targetTable);
        DataStream<Row> result = tableEnv.toAppendStream(vertexTable, Types.ROW(Types.STRING(), Types.STRING(), Types.STRING(), Types.SQL_TIMESTAMP()));
        Table groupedVertexTable = tableEnv.fromDataStream(result, "f0, f1, f2, f3.rowtime")
                .window(Slide.over(windowSize).
                        every(slideSize).
                        on("f3").
                        as("statWindow"))
                .groupBy("statWindow, f2")
                .select("f2 as vertexLabel , f0.count as vertexCount");


        DataStream<Row> rowDataStream = tableEnv.toAppendStream(edgeTable, Row.class)
                .union(tableEnv.toAppendStream(groupedVertexTable, Row.class));

        rowDataStream.flatMap(new FlatMapFunction<Row, Edge<Vertex, Vertex>>() {
            @Override
            public void flatMap(Row value, Collector<Edge<Vertex, Vertex>> out) throws Exception {
                if (value.getArity() > 2) {
                    out.collect(graphStream.addEdge(value.getField(0).toString(), value.getField(1).toString(), value.getField(2), Integer.parseInt(value.getField(3).toString())));
                }
            }

        });
        return graphStream;

    }
}
