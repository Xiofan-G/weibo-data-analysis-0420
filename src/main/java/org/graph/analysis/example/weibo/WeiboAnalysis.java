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
                .apply(new SubGraph())//����ֱ�ӻ���filter��ô�õľ���datastream����������ʹ�õľ���graphstream����
                .apply(new Grouping())//�������apply���Ի���.keyby().process()��apply�൱�ڰ�keyby��process��װ��
                .apply(new MyDataSink());

        graphStreamSource.getEnvironment().execute("Weibo Data Streaming To Graph");
    }
}
