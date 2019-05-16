package org.myorg.quickstart.sharedState;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Hashtable;


/**
 * Created by zainababbas on 07/02/2017.
 */

public class CustomKeySelector4<K, EV> implements KeySelector<Tuple3<K, EV,EV>, K> {
    private final int key1;
    private static Hashtable<Object,Object> keyMap = new Hashtable<>();

    public CustomKeySelector4(int k) {
        this.key1 = k;

    }


    public Object getValue (Object k) throws Exception {

        Object key2 = keyMap.get(k);

        return key2;

    }

    @Override
    public K getKey(Tuple3<K, EV, EV> edge) throws Exception {
        keyMap.put(edge.getField(key1),edge.getField(key1+1));
        return edge.getField(key1);

    }
}


