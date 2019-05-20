package org.myorg.quickstart.deprecated.old;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class TestValueState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<String, String>> {

    /**
     * The ValueState handle. The first field is the count, the second field a running sum.
     */
    private ValueState<Tuple2<Long, Long>> sum;

    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<String, String>> out) throws Exception {

        // access the state value
        Tuple2<Long, Long> currentState = sum.value();
        Tuple2<Long, Long> newState = new Tuple2<>();

        // update the count
        newState.f0 = input.f0;

        // add the second field of the input value
        newState.f1 = input.f1;

        // update the state
        sum.update(newState);

        // if the count reaches 2, emit the average and clear the state
            out.collect(new Tuple2<>(input.f0 + " " + input.f1, "read state value " + currentState));
            sum.clear();

    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<>(
                        "average", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}), // type information
                        Tuple2.of(0L, 0L)); // default value of the state, if nothing was set
        sum = getRuntimeContext().getState(descriptor);
    }
}


// the printed output will be (1,4) and (1,5)

