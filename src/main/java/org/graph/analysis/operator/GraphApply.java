package org.graph.analysis.operator;

import java.io.Serializable;

/**
 * High level interface to fit every graph stream stream inherited from DateStream
 *
 * @param <T> data element which specific Edge<Vertex, Vertex> in this scenario deserialize from string stream
 */
public interface GraphApply<T> extends Serializable {
    T run(T graphStream);
}
