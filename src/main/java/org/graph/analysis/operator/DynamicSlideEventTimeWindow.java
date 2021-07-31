package org.graph.analysis.operator;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.graph.analysis.entity.ControlMessage;
import org.graph.analysis.entity.Edge;
import org.graph.analysis.entity.Vertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class DynamicSlideEventTimeWindow extends WindowAssigner<Edge<Vertex, Vertex>, TimeWindow> {
    private static final long serialVersionUID = 1L;

    private long size;

    private long slide;

    private long offset;

    protected DynamicSlideEventTimeWindow(long size, long slide, long offset) {
        if (Math.abs(offset) >= slide || size <= 0) {
            throw new IllegalArgumentException("SlidingEventTimeWindows parameters must satisfy " +
                    "abs(offset) < slide and size > 0");
        }

        this.size = size;
        this.slide = slide;
        this.offset = offset;
    }

    public static DynamicSlideEventTimeWindow of(Time size, Time slide) {
        return new DynamicSlideEventTimeWindow(size.toMilliseconds(), slide.toMilliseconds(), 0);
    }

    @Override
    public Collection<TimeWindow> assignWindows(Edge<Vertex, Vertex> element, long timestamp, WindowAssignerContext context) {
        // update window size and slide size by data's control massage tag
        this.updateWindowAndSlideSize(element);
        if (timestamp > Long.MIN_VALUE) {
            List<TimeWindow> windows = new ArrayList<>((int) (size / slide));
            long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, offset, slide);
            for (long start = lastStart;
                 start > timestamp - size;
                 start -= slide) {
                windows.add(new TimeWindow(start, start + size));
            }
            return windows;
        } else {
            throw new RuntimeException("Record has Long.MIN_VALUE timestamp (= no timestamp marker). " +
                    "Is the time characteristic set to 'ProcessingTime', or did you forget to call " +
                    "'DataStream.assignTimestampsAndWatermarks(...)'?");
        }
    }

    /**
     * The Dynamic window's core logic
     * we bind the control massage up the data stream
     * then use the window size and slide size to change the flink window size and slide size
     *
     * @param element data element-> Edge<Vertex, Vertex>
     */


    public void updateWindowAndSlideSize(Edge<Vertex, Vertex> element) {
        ControlMessage controlMessage = element.getControlMessage();
        if (controlMessage == null) {
            controlMessage = ControlMessage.buildDefault();
        }
        long newSize = ControlMessage.timeOf(controlMessage.getWindowSize()).toMilliseconds();
        long newSlide = ControlMessage.timeOf(controlMessage.getSlideSize()).toMilliseconds();


        if (newSize == this.size && newSlide == this.slide) {
            return;
        }

        this.size = newSize;
        this.slide = newSlide;
        this.offset = 0;

    }

    public long getSize() {
        return size;
    }

    public long getSlide() {
        return slide;
    }

    @Override
    public Trigger<Edge<Vertex, Vertex>, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return new MyEventTimeTrigger();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return true;
    }

}
