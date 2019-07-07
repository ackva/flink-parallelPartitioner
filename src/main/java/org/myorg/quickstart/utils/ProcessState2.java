package org.myorg.quickstart.utils;

import org.apache.flink.graph.Edge;

import java.util.ArrayList;
import java.util.List;

/**
 * The data type stored in the state
 */
public class ProcessState2 {

    public long key;
    private boolean timerCalled;
    public long lastModified;

}



