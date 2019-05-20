package org.myorg.quickstart.sharedState;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.graph.Edge;
import org.myorg.quickstart.TwoPhasePartitioner.EdgeEventGelly;

import java.util.HashMap;

/**
 * Created by zainababbas on 07/02/2017.
 */

public class CustomKeySelector5<K, EV> implements KeySelector<EdgeEventGelly, K> {
    private final int key1;
    private static final HashMap<Object, Object> keyMap = new HashMap<>();

    public CustomKeySelector5 (int k) {
        this.key1 = k;
    }

    public K getKey(EdgeEventGelly edgeEvent) throws Exception {
        keyMap.put(edgeEvent.getEdge().getField(key1),edgeEvent.getEdge().getField(key1+1));
        return (K) edgeEvent.getEdge().getField(key1);
    }

    public Object getValue (Object k) throws Exception {

        Object key2 = keyMap.get(k);
        keyMap.clear();
        return key2;

    }
}