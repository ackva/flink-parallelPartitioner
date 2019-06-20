package org.myorg.quickstart.utils;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.graph.Edge;

import java.util.HashMap;

/**
 * Created by zainababbas on 07/02/2017.
 */

public class CustomKeySelector6<K, EV> implements KeySelector<Edge<Integer, Long>, K> {
    private final int key1;
    private static final HashMap<Object, Object> keyMap = new HashMap<>();

    public CustomKeySelector6(int k) {
        this.key1 = k;
    }

    public K getKey(Edge<Integer, Long> edge) throws Exception {
        keyMap.put(edge.getField(key1),edge.getField(key1+1));
        return (K) edge.getField(key1);
    }

    public Object getValue (Object k) throws Exception {

        Object key2 = keyMap.get(k);
        keyMap.clear();
        return key2;

    }
}