package org.graph.analysis.example.weibo;

import org.graph.analysis.GraphStream;
import org.graph.analysis.GraphStreamSource;
import org.graph.analysis.example.weibo.operator.WeiboDataToEdge;
import org.graph.analysis.network.Server;
import org.graph.analysis.operator.GroupingApply;
import org.graph.analysis.operator.MyDataSink;
import org.graph.analysis.operator.StreamToGraph;
import org.graph.analysis.operator.SubGraphApply;

public class WeiboAnalysis {
    public static void main(String[] args) throws Exception {
        Server.initWebSocketServer();

        String groupId = "weibo";
        String topic = "weibo";
        StreamToGraph<String> mapFunc = new WeiboDataToEdge();
        GraphStreamSource graphStreamSource = new GraphStreamSource();
        GraphStream weiboGraph = graphStreamSource.fromKafka(groupId, topic, mapFunc);
        weiboGraph
                .apply(new SubGraphApply(weiboGraph.getVertexFilter(), weiboGraph.getVertexFilter()))
                .apply(new GroupingApply(weiboGraph.isGrouping()))
                .addSink(new MyDataSink(weiboGraph));

        graphStreamSource.getEnvironment().execute("Citibank Data Streaming To Graph");
    }
}
