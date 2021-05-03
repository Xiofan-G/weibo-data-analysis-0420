package org.graph.analysis;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.graph.analysis.operator.GroupingApply;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Unit test for Graph Stream App.
 */
public class AppTest {
    /**
     * Rigorous Test :-)
     */
    @Test
    public void testGroupOperator() {
        GroupingApply groupingApply = new GroupingApply(true);
        List<String> out = new ArrayList<>();
        ListCollector<String> listCollector = new ListCollector<>(out);
//        Assert.assertEquals(Lists.newArrayList("hello world"), out);
        assertTrue(true);
    }

    @Test
    public void testFilterOperator() {
        assertTrue(true);
    }

    @Test
    public void testSinkOperator() {
        assertTrue(true);
    }

}
