package org.graph.analysis.operator;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.graph.analysis.GraphStream;
import org.graph.analysis.entity.ControlMessage;
import org.graph.analysis.entity.Edge;
import org.graph.analysis.entity.GraphContainer;
import org.graph.analysis.entity.Vertex;
import org.graph.analysis.network.Server;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Iterator;

public class MyDataSink implements GraphApply<GraphStream>, SinkFunction<Edge<Vertex, Vertex>>, CheckpointedFunction {
    /**
     * Hold a appropriate number of element to send frontend
     */
    protected int threshold = 20;
    protected GraphContainer graphContainer = new GraphContainer();
    /**
     * Hold the timestamp when send to frontend
     */
    protected Long lastSunkAt = new Date().getTime();
    /**
     * Grouping and without Grouping will use different method to deal with serializing
     */
    private transient ListState<Boolean> withGroupingState;
    /**
     * cooperate with lastSunkAt, when data's timestamp is large than lastSunkAt + slideSize
     * Just do sinking,send data to frontend
     */
    private transient ListState<Long> slideSizeState;


    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Boolean> withGroupingStateDescriptor = new ListStateDescriptor<>(ControlMessage.withGroupStateName, BasicTypeInfo.BOOLEAN_TYPE_INFO);
        ListStateDescriptor<Long> slideSizeStateDescriptor = new ListStateDescriptor<>(ControlMessage.slideSizeStateName, BasicTypeInfo.LONG_TYPE_INFO);

        withGroupingState = context.getOperatorStateStore().getListState(withGroupingStateDescriptor);
        slideSizeState = context.getOperatorStateStore().getListState(slideSizeStateDescriptor);
        if (slideSizeState.get() == null) {
            slideSizeState.add(ControlMessage.getDefaultSlideSize().toMilliseconds());
        }
    }

    @Override
    public void invoke(Edge<Vertex, Vertex> value, Context context) throws Exception {
        if (!this.updateWithGroupingState(value)) {
            //需要分组信息，因为edge的对象不同，所以需要判断
            Iterator<Boolean> iterator = this.withGroupingState.get().iterator();
            if (iterator.hasNext() && iterator.next()) {
                //图容器把边都收纳在一起
                //因为做了keyby，所以在每个key上有8个并行点
                graphContainer.addEdge(value.getSource().getLabel(), value.getTarget().getLabel(), value.getLabel(), value.getCount());
            } else {
                graphContainer.addEdge(value);
            }
            threshold = 100;
        } else {
            // Need send data immediately
            threshold = 1;
        }

        long timestamp = new Timestamp(System.currentTimeMillis()).getTime();
        send(timestamp);
    }

    protected void send(long timestamp) throws Exception {
        //判断一下上次更新+slidesize的值小于当前的时间，那么这个数据可以做sink
        //这个处理可以减轻前端渲染数据的压力
        //不会在一瞬间接收过多的推送，否则前端会卡
        if (lastSunkAt + slideSizeState.get().iterator().next() <= timestamp) {
            Server.sendToAll(graphContainer.toString());
            graphContainer.clear();
            lastSunkAt = timestamp;
            return;
        }
        //设置了一个100的数量阈值，防止数据量爆发，在下次更新的时候一起推送，造成崩溃
        //如果controlmessage进行变化，则阈值变成1，则立马调用这部分将之前的数据推送出去
        //因为不重启，如果改变信息的时候，还有信息在container里面，这一部分数据已经统计好了，但不是根据已改变的信息统计的，
        //所以需要赶紧把数据吐出来，以免影响之后的统计
        //所以造成前端的图可能需要两三个slide的时间才能把图更新出来
        if (graphContainer.size() >= threshold) {
            Server.sendToAll(graphContainer.toString());
            graphContainer.clear();
        }
    }

    /**
     * Update 3 states from data's tag
     *
     * @param value data element
     * @return need send immediately or not
     * @throws Exception
     */
    private boolean updateWithGroupingState(Edge<Vertex, Vertex> value) throws Exception {

        boolean oldWithGrouping = false;
        if (this.withGroupingState.get().iterator().hasNext()) {
            oldWithGrouping = this.withGroupingState.get().iterator().next();
        }

        this.withGroupingState.clear();
        this.slideSizeState.clear();

        ControlMessage controlMessage = value.getControlMessage();

        this.withGroupingState.add(controlMessage.getWithGrouping());
        this.slideSizeState.add(ControlMessage.timeOf(controlMessage.getSlideSize()).toMilliseconds());

        return oldWithGrouping != this.withGroupingState.get().iterator().next();
    }

    @Override
    public GraphStream run(GraphStream graphStream) {
        graphStream.addSink(this);
        return graphStream;
    }
}

