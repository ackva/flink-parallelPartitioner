package org.myorg.quickstart.utils;

import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;

import java.util.ArrayList;
import java.util.List;

/**
 * The data type stored in the state
 */
public class ProcessState {

    public Edge key;
    public List<Edge<Integer, NullValue>> edgeList = new ArrayList<>();
    public long lastModified;
    public int repetition = 0;
}