package org.graph.analysis.entity;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public class ControlMessage implements Serializable {
    public static final String controlLabel = "Control";

    public static final String edgeFilterStateName = "edgeFilter";
    public static final String vertexFilterStateName = "vertexFilter";
    public static final String withGroupStateName = "withGroup";
    public static final String slideSizeStateName = "slideSize";

    public static final String defaultWindowSize = "30.seconds";
    public static final String defaultSlideSize = "10.seconds";


    private static final long serialVersionUID = 1L;
    private Boolean withGrouping = false;
    private String windowSize;
    private String slideSize;
    private String vertexLabel;
    private String edgeLabel;
    private Long timestamp;

    public ControlMessage() {
        withGrouping = false;
    }

    public static MapStateDescriptor<String, ControlMessage> getDescriptor() {
        return new MapStateDescriptor<>(
                ControlMessage.controlLabel,
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<ControlMessage>() {
                }));
    }

    public static ControlMessage buildFromString(String controlSignal) {
        return JSON.parseObject(controlSignal, ControlMessage.class);
    }

    public static Time timeOf(String commaSplitTimeStr) {
        String[] timeArray = commaSplitTimeStr.split("\\.");
        long size = Long.parseLong(timeArray[0]);
        String unit = timeArray[1].toUpperCase(); // eg: "MINUTES" or "SECONDS"

        return Time.of(size, TimeUnit.valueOf(unit));
    }

    public static Time getDefaultWindowSize() {
        return ControlMessage.timeOf(ControlMessage.defaultWindowSize);
    }

    public static Time getDefaultSlideSize() {
        return ControlMessage.timeOf(ControlMessage.defaultSlideSize);
    }

    public static ControlMessage buildDefault() {
        ControlMessage controlMessage = new ControlMessage();
        controlMessage.setWithGrouping(false);
        controlMessage.setWindowSize(defaultWindowSize);
        controlMessage.setSlideSize(defaultSlideSize);
        controlMessage.setVertexLabel(null);
        controlMessage.setEdgeLabel(null);
        return controlMessage;
    }

    public void copy(ControlMessage controlMessage) {
        this.edgeLabel = controlMessage.edgeLabel;
        this.vertexLabel = controlMessage.edgeLabel;
        this.slideSize = controlMessage.slideSize;
        this.windowSize = controlMessage.windowSize;
        this.withGrouping = controlMessage.withGrouping;
    }

    public Boolean getWithGrouping() {
        return withGrouping;
    }

    public void setWithGrouping(Boolean withGrouping) {
        this.withGrouping = withGrouping;
    }

    public String getWindowSize() {
        return windowSize;
    }

    public void setWindowSize(String windowSize) {
        this.windowSize = windowSize;
    }

    public String getSlideSize() {
        return slideSize;
    }

    public void setSlideSize(String slideSize) {
        this.slideSize = slideSize;
    }

    public String getVertexLabel() {
        return vertexLabel;
    }

    public void setVertexLabel(String vertexLabel) {
        this.vertexLabel = vertexLabel;
    }

    public String getEdgeLabel() {
        return edgeLabel;
    }

    public void setEdgeLabel(String edgeLabel) {
        this.edgeLabel = edgeLabel;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return String.format("{" +
                        "\"label\":\"%s\"," +
                        "\"windowSize\":\"%s\"," +
                        "\"slideSize\":\"%s\"," +
                        "\"vertexLabel\":\"%s\"," +
                        "\"edgeLabel\":\"%s\"," +
                        "\"withGrouping\":\"%s\"," +
                        "\"timestamp\":\"%d\"" +
                        "}",
                controlLabel, windowSize, slideSize, vertexLabel, edgeLabel, withGrouping, timestamp);
    }

    public String getId() {
        return String.format("%s-%s-%s-%s-%s", windowSize, slideSize, vertexLabel, edgeLabel, withGrouping);
    }
}
