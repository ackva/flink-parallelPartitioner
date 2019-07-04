package org.myorg.quickstart.utils;

        import org.apache.flink.streaming.api.functions.sink.SinkFunction;
        import scala.Int;

/**
 * Created by zainababbas on 29/03/2017.
 */
public class DumSink4 implements SinkFunction<DisjointSet<Integer>> {

    private static final long serialVersionUID = 1876986644706201196L;


    public DumSink4() {

    }


    @Override
    public void invoke(DisjointSet<Integer> longDisjointSet) throws Exception {

    }
}