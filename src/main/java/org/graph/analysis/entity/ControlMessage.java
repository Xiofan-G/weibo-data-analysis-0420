package org.graph.analysis.entity;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.Serializable;
import java.util.Objects;

public class ControlMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final String controlLabel = "Control";
    public Boolean withGrouping = false;
    public String windowSize;
    public String slideSize;
    public String vertexLabel;
    public String edgeLabel;
    public Long timestamp;

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

    public static ControlMessage buildDefault(String windowSize, String slideSize) {
        ControlMessage controlMessage = new ControlMessage();
        controlMessage.setWithGrouping(false);
        controlMessage.setWindowSize(windowSize);
        controlMessage.setSlideSize(slideSize);
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

    public Boolean needLoop(String windowSize, String slideSize) {
        if (!windowSize.equals(this.windowSize))
            return true;
        if (!slideSize.equals(this.slideSize))
            return true;

        return (!Objects.isNull(this.edgeLabel) ||
                !Objects.isNull(this.vertexLabel) ||
                this.withGrouping
        );

    }

    public Boolean isWithGrouping() {
        return withGrouping;
    }

    public void setWithGrouping(boolean withGrouping) {
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
}
