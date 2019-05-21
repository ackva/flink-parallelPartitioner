package org.myorg.quickstart.deprecated;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.myorg.quickstart.deprecated.BroadcastState.tupleTypeInfo;

public class MatchFunctionDepr extends KeyedBroadcastProcessFunction<BroadcastState.Color, Item, Tuple2<BroadcastState.Shape, BroadcastState.Shape>, String> {

    private StateDepr stateDepr;
    private int counter = 0;

    private final MapStateDescriptor<String, List<Item>> matchStateDesc = new MapStateDescriptor<>("items", BasicTypeInfo.STRING_TYPE_INFO, new ListTypeInfo<>(Item.class));

    private final MapStateDescriptor<String, Tuple2<BroadcastState.Shape, BroadcastState.Shape>> broadcastStateDescriptor = new MapStateDescriptor<>("RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, tupleTypeInfo);

/*
    public MatchFunctionDepr (StateDepr stateDepr) {
        this.stateDepr = stateDepr;
    }*/


    // Process new rule
    @Override
    public void processBroadcastElement(Tuple2<BroadcastState.Shape, BroadcastState.Shape> value, Context ctx, Collector<String> out) throws Exception {
        ctx.getBroadcastState(broadcastStateDescriptor).put("Rule_" + counter++, value);
        System.out.println("ADDED: Rule_" + (counter-1) + " " + value);
    }

    // Process item (shape, color)
    @Override
    public void processElement(Item nextItem, ReadOnlyContext ctx, Collector<String> out) throws Exception {

        final MapState<String, List<Item>> partialMatches = getRuntimeContext().getMapState(matchStateDesc);
        final BroadcastState.Shape shapeOfNextItem = nextItem.getShape();

        System.out.println("SAW: " + nextItem);
        for (Map.Entry<String, Tuple2<BroadcastState.Shape, BroadcastState.Shape>> entry: ctx.getBroadcastState(broadcastStateDescriptor).immutableEntries()) {
            final String ruleName = entry.getKey();
            final Tuple2<BroadcastState.Shape, BroadcastState.Shape> rule = entry.getValue();

            List<Item> partialsForThisRule = partialMatches.get(ruleName);
            if (partialsForThisRule == null) {
                partialsForThisRule = new ArrayList<>();
            }

            if (shapeOfNextItem == rule.f1 && !partialsForThisRule.isEmpty()) {
                for (Item i : partialsForThisRule) {
                    out.collect("MATCH: " + i + " - " + nextItem);
                    out.collect("MATCH: " + i + " add new rule");


                }
                partialsForThisRule.clear();
            }

            if (shapeOfNextItem == rule.f0) {
                partialsForThisRule.add(nextItem);
            }

            if (partialsForThisRule.isEmpty()) {
                partialMatches.remove(ruleName);
            } else {
                partialMatches.put(ruleName, partialsForThisRule);
            }
        }
    }
}
