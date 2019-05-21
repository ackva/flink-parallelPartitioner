package org.myorg.quickstart.deprecated;

/*
 * Copyright 2018 data Artisans GmbH
*/

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.EnumTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class BroadcastState {

    public enum Shape {
        RECTANGLE, TRIANGLE, CIRCLE
    }

    public enum Color {
        RED, GREEN, BLUE
    }

    final static Class<Tuple2<Shape, Shape>> typedTuple = (Class<Tuple2<Shape, Shape>>) (Class<?>) Tuple2.class;

    final static TupleTypeInfo<Tuple2<Shape, Shape>> tupleTypeInfo = new TupleTypeInfo<>(
            typedTuple,
            new EnumTypeInfo<>(Shape.class),
            new EnumTypeInfo<>(Shape.class)
    );

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // read input

        // rules based on input

        // collection of rules / or input

        // broadcast rules

        // process 2nd batch

        // iterate until end of file


        StateDepr stateDepr = new StateDepr();
        List<Tuple2<Shape, Shape>> rules = stateDepr.getRules();
        stateDepr.addRule(Shape.CIRCLE,Shape.CIRCLE);

        final List<Item> keyedInput = new ArrayList<>();
        keyedInput.add(new Item(Shape.RECTANGLE, Color.GREEN));
        keyedInput.add(new Item(Shape.TRIANGLE, Color.BLUE));
        keyedInput.add(new Item(Shape.TRIANGLE, Color.RED));
        keyedInput.add(new Item(Shape.CIRCLE, Color.BLUE));
        keyedInput.add(new Item(Shape.CIRCLE, Color.GREEN));
        keyedInput.add(new Item(Shape.TRIANGLE, Color.BLUE));
        keyedInput.add(new Item(Shape.RECTANGLE, Color.GREEN));
        keyedInput.add(new Item(Shape.CIRCLE, Color.GREEN));
        keyedInput.add(new Item(Shape.CIRCLE, Color.BLUE));
        keyedInput.add(new Item(Shape.CIRCLE, Color.BLUE));
        keyedInput.add(new Item(Shape.TRIANGLE, Color.GREEN));
        keyedInput.add(new Item(Shape.RECTANGLE, Color.GREEN));
        keyedInput.add(new Item(Shape.RECTANGLE, Color.GREEN));
        keyedInput.add(new Item(Shape.RECTANGLE, Color.GREEN));
        //



        MapStateDescriptor<String, Tuple2<Shape, Shape>> rulesStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                tupleTypeInfo
        );

        KeyedStream<Item, Color> itemColorKeyedStream = env.fromCollection(keyedInput)
                //.rebalance()                               // needed to increase the parallelism
                .map(item -> item)
                .setParallelism(4)
                .keyBy(item -> item.getColor());

        BroadcastStream<Tuple2<Shape, Shape>> broadcastRulesStream = env.fromCollection(rules)
                .flatMap(new FlatMapFunction<Tuple2<Shape, Shape>, Tuple2<Shape, Shape>>() {
                    private static final long serialVersionUID = 6462244253439410814L;

                    @Override
                    public void flatMap(Tuple2<Shape, Shape> value, Collector<Tuple2<Shape, Shape>> out) {
                        out.collect(value);
                    }
                })
                .setParallelism(4)
                .broadcast(rulesStateDescriptor);

        DataStream<String> output = itemColorKeyedStream
                .connect(broadcastRulesStream)
                .process(new MatchFunctionDepr());

        stateDepr.addRule(Shape.RECTANGLE,Shape.RECTANGLE);


        output.print();
        System.out.println(env.getExecutionPlan());
        env.execute();
    }

}