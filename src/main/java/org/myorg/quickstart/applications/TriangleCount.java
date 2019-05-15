package org.myorg.quickstart.applications;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

public class TriangleCount {



    // *** Transformation Methods *** //

    /**
     * Receives 2 tuples from the same edge (src + target) and intersects the attached neighborhoods.
     * For each common neighbor, increase local and global counters.
     */
    public static final class IntersectNeighborhoods implements
            FlatMapFunction<Tuple3<Integer, Integer, TreeSet<Integer>>, Tuple2<Integer, Integer>> {

        Map<Tuple2<Integer, Integer>, TreeSet<Integer>> neighborhoods = new HashMap<>();

        public void flatMap(Tuple3<Integer, Integer, TreeSet<Integer>> t, Collector<Tuple2<Integer, Integer>> out) {

            //intersect neighborhoods and emit local and global counters
            Tuple2<Integer, Integer> key = new Tuple2<>(t.f0, t.f1);
            if (neighborhoods.containsKey(key)) {
                // this is the 2nd neighborhood => intersect
                TreeSet<Integer> t1 = neighborhoods.remove(key);
                TreeSet<Integer> t2 = t.f2;
                int counter = 0;
                if (t1.size() < t2.size()) {
                    // iterate t1 and search t2
                    for (int i : t1) {
                        if (t2.contains(i)) {
                            counter++;
                            out.collect(new Tuple2<>(i, 1));
                        }
                    }
                } else {
                    // iterate t2 and search t1
                    for (int i : t2) {
                        if (t1.contains(i)) {
                            counter++;
                            out.collect(new Tuple2<>(i, 1));
                        }
                    }
                }
                if (counter > 0) {

                    //emit counter for srcID, trgID, and total
                    out.collect(new Tuple2<>(t.f0, counter));
                    out.collect(new Tuple2<>(t.f1, counter));
                    // -1 signals the total counter
                    out.collect(new Tuple2<>(-1, counter));
                }
            } else {
                // first neighborhood for this edge: store and wait for next
                neighborhoods.put(key, t.f2);
            }
        }
    }

    /**
     * Sums up and emits local and global counters.
     */
    public static final class SumAndEmitCounters implements FlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
        Map<Integer, Integer> counts = new HashMap<>();
        public static int count=0;

        public void flatMap(Tuple2<Integer, Integer> t, Collector<Tuple2<Integer, Integer>> out) {

            if (counts.containsKey(t.f0)) {
                count++;
                System.out.println("count"+count+"count");
                int newCount = counts.get(t.f0) + t.f1;
                counts.put(t.f0, newCount);
                out.collect(new Tuple2<>(t.f0, newCount));
            } else {
                //count++;
                //System.out.println("count"+count+"count");
                counts.put(t.f0, t.f1);
                out.collect(new Tuple2<>(t.f0, t.f1));
            }
        }
    }

    public static final class ProjectCanonicalEdges implements
            MapFunction<Tuple3<Integer, Integer, TreeSet<Integer>>, Tuple3<Integer, Integer, TreeSet<Integer>>> {

        @Override
        public Tuple3<Integer, Integer, TreeSet<Integer>> map(Tuple3<Integer, Integer, TreeSet<Integer>> t) {

            int source = Math.min(t.f0, t.f1);
            int trg = Math.max(t.f0, t.f1);
            t.setField(source, 0);
            t.setField(trg, 1);
            return t;
        }
    }

}
