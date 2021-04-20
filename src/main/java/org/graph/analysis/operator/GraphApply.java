package org.graph.analysis.operator;

import java.io.Serializable;

public interface GraphApply<T> extends Serializable {
    T run(T graphStream);
}
