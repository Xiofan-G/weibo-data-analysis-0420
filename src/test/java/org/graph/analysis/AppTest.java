package org.graph.analysis;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;
import org.graph.analysis.entity.ControlMessage;
import org.graph.analysis.entity.Edge;
import org.graph.analysis.entity.Vertex;
import org.graph.analysis.example.weibo.operator.WeiboDataToEdge;
import org.graph.analysis.operator.Grouping;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for Graph Stream App.
 */
public class AppTest extends AbstractTestBase {
    /**
     * Rigorous Test :-)
     */
//grouping operator test

    @Test
    public void testGroupOperator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stringDataStream = env.fromElements("{\"id\":\"926-65-2447\",\"label\":\"Author\",\"remark\":\"Weibo\",\"source\":{\"id\":\"016-47-5173\",\"label\":\"User\",\"name\":\"Dr. Genie Ebert\", \"city\":\"East Roycemouth\", \"age\":28, \"gender\":\"male\"}, \"target\":{\"id\":\"600-41-0682\",\"label\":\"Weibo\",\"name\":\"No, homo habilis was erect. Australopithecus was never fully erect.\",\"agent\":\"Android 7.0\"}, \"timestamp\":\"1621851652216\"}").setParallelism(1);
        WeiboDataToEdge weiboDataToEdge = new WeiboDataToEdge();
        DataStream<Edge<Vertex, Vertex>> edgeDataStream = stringDataStream.flatMap(weiboDataToEdge).flatMap(new FlatMapFunction<Edge<Vertex, Vertex>, Edge<Vertex, Vertex>>() {
            @Override
            public void flatMap(Edge<Vertex, Vertex> value, Collector<Edge<Vertex, Vertex>> out) throws Exception {
                ControlMessage controlMessage = ControlMessage.buildDefault();
                //Simulate the process of combining broadcast streams
                controlMessage.setWithGrouping(true);
                value.setControlMessage(controlMessage);
                out.collect(value);
            }
        });
        //DataStream to GraphStream
        GraphStream graphStream = new GraphStream(env, edgeDataStream.getTransformation());
        Grouping groupingApply = new Grouping();
        //addink, get the value and put together to a string, compare by equals, if equal then the test is successful
        groupingApply.run(graphStream).addSink(new SinkFunction<Edge<Vertex, Vertex>>() {
            @Override
            public void invoke(Edge<Vertex, Vertex> value, Context context) throws Exception {
                String result = value.getLabel() + "-" + value.getSource().getLabel() + "-" + value.getTarget().getLabel() + "-" + value.getCount();
                assertEquals("Author-User-Weibo-1", result);
            }
        });

        env.execute("test grouping ");

    }

    @Test
    public void testFilterOperator() {
        assertTrue(true);
    }

    @Test
    public void testSinkOperator() {
        assertTrue(true);
    }


    @Test
    public void testDynamicWindowOperator() {
        assertTrue(true);
    }
}
