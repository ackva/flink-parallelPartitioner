package org.myorg.quickstart.deprecated;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

public class State {
    private ArrayList<Tuple2<BroadcastState.Shape, BroadcastState.Shape>> rules = new ArrayList<>();
    public State() {
        this.addRule(BroadcastState.Shape.RECTANGLE, BroadcastState.Shape.CIRCLE);
    }

    public void addRule(BroadcastState.Shape one, BroadcastState.Shape two) {
        this.rules.add(new Tuple2<>(one, two));
    }
    public ArrayList<Tuple2<BroadcastState.Shape, BroadcastState.Shape>> getRules() {
        return this.rules;
    }
}
