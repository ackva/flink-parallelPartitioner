package org.myorg.quickstart.utils;

import org.apache.flink.graph.Edge;

import java.util.ArrayList;
import java.util.List;

/**
 * The data type stored in the state
 */
public class ProcessListState {

    public List<Long> key;
    //public List<Edge<Integer, Long>> edgeList = new ArrayList<>();
    public long lastModified;
    public int repetition = 0;
}