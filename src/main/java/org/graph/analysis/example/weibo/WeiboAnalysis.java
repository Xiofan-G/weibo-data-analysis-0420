package org.graph.analysis.example.weibo;

import org.graph.analysis.GraphStream;
import org.graph.analysis.GraphStreamSource;
import org.graph.analysis.example.weibo.operator.WeiboDataToEdge;
import org.graph.analysis.network.Server;
import org.graph.analysis.operator.Grouping;
import org.graph.analysis.operator.MyDataSink;
import org.graph.analysis.operator.StreamToGraph;
import org.graph.analysis.operator.SubGraph;

public class WeiboAnalysis {
    public static void main(String[] args) throws Exception {
        Server.initWebSocketServer();

        String groupId = "weibo";
        String topic = "weibo";
        StreamToGraph<String> mapFunc = new WeiboDataToEdge();
        GraphStreamSource graphStreamSource = new GraphStreamSource();
        GraphStream weiboGraph = graphStreamSource.fromKafka(groupId, topic, mapFunc);
        weiboGraph
                .apply(new SubGraph())//可以直接换成filter那么用的就是datastream的流，现在使用的就是graphstream的流
                .apply(new Grouping())//如果不用apply可以换成.keyby().process()，apply相当于把keyby和process封装了
                .apply(new MyDataSink());

        graphStreamSource.getEnvironment().execute("Weibo Data Streaming To Graph");
    }
}
