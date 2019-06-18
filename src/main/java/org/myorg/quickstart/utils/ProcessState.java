package org.myorg.quickstart.utils;

import org.apache.flink.graph.Edge;

import java.util.ArrayList;
import java.util.List;

/**
 * The data type stored in the state
 */
public class ProcessState {

    public long key;
    public Edge edge;
    public long lastModified;
}