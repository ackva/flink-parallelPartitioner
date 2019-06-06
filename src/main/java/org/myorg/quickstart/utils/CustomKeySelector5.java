package org.myorg.quickstart.utils;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;
import org.myorg.quickstart.utils.EdgeEventGelly;

import java.util.HashMap;

/**
 * Created by zainababbas on 07/02/2017.
 */

public class CustomKeySelector5<K, EV> implements KeySelector<Edge<Integer, NullValue>, K> {
    private final int key1;
    private static final HashMap<Object, Object> keyMap = new HashMap<>();

    public CustomKeySelector5 (int k) {
        this.key1 = k;
    }

    public K getKey(Edge<Integer, NullValue> edge) throws Exception {
        keyMap.put(edge.getField(key1),edge.getField(key1+1));
        return (K) edge.getField(key1);
    }

    public Object getValue (Object k) throws Exception {

        Object key2 = keyMap.get(k);
        keyMap.clear();
        return key2;

    }
}