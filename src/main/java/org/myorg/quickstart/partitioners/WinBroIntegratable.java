package org.myorg.quickstart.partitioners;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.OutputTag;
import org.myorg.quickstart.applications.SimpleEdgeStream;
import org.myorg.quickstart.jobstatistics.LoadBalanceCalculator;
import org.myorg.quickstart.jobstatistics.VertexCut;
import org.myorg.quickstart.partitioners.matchFunctions.MatchFunctionWinHash;
import org.myorg.quickstart.partitioners.matchFunctions.MatchFunctionWinHash2;
import org.myorg.quickstart.partitioners.matchFunctions.MatchFunctionWindowHash;
import org.myorg.quickstart.partitioners.windowFunctions.ProcessWindowDegreeHashed;
import org.myorg.quickstart.partitioners.windowFunctions.ProcessWindowGellyHashValue;
import org.myorg.quickstart.utils.CustomKeySelector6;
import org.myorg.quickstart.utils.HashPartitioner;
import org.myorg.quickstart.utils.TEMPGLOBALVARIABLES;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class WinBroIntegratable {

    public static final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

    //public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    private final static Class<Tuple2<Integer, ArrayList<Integer>>> typedTuple = (Class<Tuple2<Integer, ArrayList<Integer>>>) (Class<?>) Tuple2.class;
    private final static TupleTypeInfo tupleTypeInfo = new TupleTypeInfo<>(typedTuple,new GenericTypeInfo<>(Integer.class),new GenericTypeInfo<>(Integer.class));

    // Static variables for debugging, testing, etc.
    public static StreamExecutionEnvironment env;
    public static long windowSizeInMs = 0;
    public static long wait = 0; //public static long sleep = windowSizeInMs/100;
    private static String inputPath = null;
    private static String algorithm = "";
    public static int keyParam = 0;
    private static int globalPhase = 0;
    public static String graphName;
    public static int k = 2; // parallelism - partitions
    public static double lambda = 1.0;
    public static int stateDelay = 0;

    public WinBroIntegratable(
            StreamExecutionEnvironment env, String inputPath, String algorithm, int keyParam, int k, int globalPhase, long windowSizeInMs,
            int stateDelay) throws Exception {
        this.env = env;
        this.inputPath = inputPath;
        this.algorithm = algorithm;
        this.keyParam = keyParam;
        this.k = k;
        this.globalPhase = globalPhase;
        this.windowSizeInMs = windowSizeInMs;
        this.stateDelay = stateDelay;
    }


    public DataStream<Edge<Integer,NullValue>> partitionGraph() throws Exception {

        long windowSize2 = windowSizeInMs * 2;
        String timestamp = new SimpleDateFormat("yy-MM-dd_HH-mm-ss").format(new Date());

        ProcessWindowGellyHashValue firstPhaseProcessor = new ProcessWindowGellyHashValue();
        MatchFunctionWinHash2 matchFunction = new MatchFunctionWinHash2(algorithm, k, lambda);
        MapStateDescriptor<String, Tuple2<Integer, ArrayList<Integer>>> rulesStateDescriptor = new MapStateDescriptor<>("RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO,tupleTypeInfo);


        //System.out.println(new SimpleDateFormat("HH:mm:ss.SSS").format(new Date()) + " timestamp for whatever you want");

        // Environment setup
        env.setParallelism(k);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        KeySelector<Edge<Integer, Long>, Integer> ks = new KeySelector<Edge<Integer, Long>, Integer>() {
            @Override
            public Integer getKey(Edge<Integer, Long> value) throws Exception {
                //System.out.println(value.f0 % 10);
                if (keyParam > 1)
                    return (value.f0 % keyParam);
                else
                    return value.f0;

            }
        };

        // Create a data stream (read from file)
        DataStream<Edge<Integer, Long>> edges = getDataStream(env);

        DataStream<Edge<Integer,NullValue>> partitionedEdges = null;


        // *** PHASE 1 ***
        //Process edges to build the local model for broadcast
        DataStream<Tuple2<HashMap<Integer, Integer>,Long>> phaseOneStream = edges
                .keyBy(ks)
                .timeWindow(Time.milliseconds(windowSizeInMs))
                .process(new ProcessWindowDegreeHashed());

        // Process edges in the similar time windows to "wait" for phase 2
        DataStream<Edge<Integer, Long>> edgesWindowed = edges
                .keyBy(ks)
                .timeWindow(Time.milliseconds(windowSizeInMs))
                .process(firstPhaseProcessor);

        // *** Phase 2 ***
        // Broadcast local state from Phase 1 to all Task Managers
        BroadcastStream<Tuple2<HashMap<Integer, Integer>,Long>> broadcastStateStream = phaseOneStream
                .broadcast(rulesStateDescriptor);

        // Connect Broadcast Stream and EdgeDepr Stream to build global model
        SingleOutputStreamOperator<Tuple2<Edge<Integer, NullValue>, Integer>> phaseTwoStream = edgesWindowed
                .keyBy(new KeySelector<Edge<Integer, Long>, Integer>() {
                    @Override
                    public Integer getKey(Edge value) throws Exception {
                        return Integer.parseInt(value.f0.toString());
                    }
                })
                .connect(broadcastStateStream)
                .process(matchFunction).setParallelism(globalPhase);
        //phaseTwoStream.print();

        DataStream<String> sideOutputStream = phaseTwoStream.getSideOutput(outputTag);
        sideOutputStream.print();

        // Final Step -- Custom Partition, based on pre-calculated ID
        partitionedEdges = phaseTwoStream
                .partitionCustom(new PartitionByTag(), 1)
                .map(new MapFunction<Tuple2<Edge<Integer,NullValue>, Integer>, Edge<Integer,NullValue>>() {
                    public Edge<Integer,NullValue> map(Tuple2<Edge<Integer,NullValue>, Integer> input) {
                        return input.f0;
                    }});

        return partitionedEdges;
        //partitionedEdges.print();

    }

    public static class PartitionByTag implements Partitioner<Integer> {
        @Override
        public int partition(Integer key, int numPartitions) {
            return key % numPartitions;
        }
    }


    private static SimpleEdgeStream<Integer, Long> getGraphStream(StreamExecutionEnvironment env) {

        long nextPrime = 4294967311L;

        return new SimpleEdgeStream<>(env.readTextFile(inputPath)
                .flatMap(new FlatMapFunction<String, Edge<Integer, Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Edge<Integer, Long>> out) throws InterruptedException {
                        String[] fields = s.replaceAll(","," ").split(" ");
                        if (!fields[0].equals("%")) {
                            int src = Integer.parseInt(fields[0]);
                            int trg = Integer.parseInt(fields[1]);
                            int srcHash = MathUtils.murmurHash(src);
                            int trgHash = MathUtils.murmurHash(trg);
                            long value = srcHash * nextPrime * trgHash;
                            out.collect(new Edge<>(src, trg, value));
                        }
                    }
                }), env);
    }

    public static  DataStream<Edge<Integer, Long>> getDataStream(StreamExecutionEnvironment env) throws IOException {

        long nextPrime = 4294967311L;


        return env.readTextFile(inputPath)
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return !value.contains("%");
                    }
                })
                .map(new MapFunction<String, Edge<Integer, Long>>() {
                    @Override
                    public Edge<Integer, Long> map(String s) throws Exception {
                        String[] fields = s.replaceAll(","," ").split(" ");
                        int src = Integer.parseInt(fields[0]);
                        int trg = Integer.parseInt(fields[1]);
                        int srcHash = MathUtils.murmurHash(src);
                        int trgHash = MathUtils.murmurHash(trg);
                        long value = srcHash * nextPrime * trgHash;
                        return new Edge<>(src, trg, value);
                    }
                });

    }


}

