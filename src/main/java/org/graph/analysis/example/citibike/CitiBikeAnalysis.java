package org.graph.analysis.example.citibike;

import org.graph.analysis.GraphStream;
import org.graph.analysis.GraphStreamSource;
import org.graph.analysis.example.citibike.operator.CitiBikeDataToEdge;
import org.graph.analysis.operator.GroupingApply;
import org.graph.analysis.operator.MyDataSink;
import org.graph.analysis.operator.StreamToGraph;
import org.graph.analysis.operator.SubGraphApply;

public class CitiBikeAnalysis {
    public static void main(String[] args) throws Exception {
        String groupId = "citibank";
        String topic = "bank";
        StreamToGraph<String> mapFunc = new CitiBikeDataToEdge();
        GraphStreamSource graphStreamSource = new GraphStreamSource();
        GraphStream citibankGraph = graphStreamSource.fromKafka(groupId, topic, mapFunc);
        citibankGraph
                .apply(new SubGraphApply(citibankGraph.getVertexFilter(), citibankGraph.getEdgeFilter()))
                .apply(new GroupingApply(citibankGraph.isGrouping()))
                .addSink(new MyDataSink(citibankGraph));

        graphStreamSource.getEnvironment().execute("Citibank Data Streaming To Graph");
    }
}
