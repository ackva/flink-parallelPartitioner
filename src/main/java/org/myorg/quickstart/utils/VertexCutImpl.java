package org.myorg.quickstart.utils;

import org.apache.flink.api.java.ExecutionEnvironment;
/*import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;*/

import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by zainababbas on 21/02/2017.
 */
public class VertexCutImpl {

    public static void main(String[] args) throws Exception {
        if (!parseParameters(args)) {
            return;
        }
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        HashMap<Long, List<Long>> T = new HashMap<>();
        for (int i = 1; i <= k; i++) {
            FileReader fr=new FileReader(InputPath+"/"+i);
            BufferedReader br = new BufferedReader(fr);
            try {
                String line;
                line = br.readLine();
                while (line != null) {
                    String[] fields = line.split("\\,");
                    String f0 = fields[0].replaceAll("\\(","").replaceAll("\\)","");
                    String f1 = fields[1].replaceAll("\\(","").replaceAll("\\)","");
                    Long src = Long.parseLong(f0);
                    Long trg = Long.parseLong(f1);
                    if (!T.containsKey(src)) {
                        T.put(src, new ArrayList<>());
                    }

                    if (!T.containsKey(trg)) {
                        T.put(trg, new ArrayList<>());

                    }

                    // be sure to read the next line otherwise you'll get an infinite loop
                    line = br.readLine();
                }
            } finally {
                // you should close out the BufferedReader
                br.close();
            }
        }

        for (int i = 1; i <= k; i++) {
            //2. URI of the file to be read
            FileReader fr=new FileReader(InputPath+"/"+i);
            BufferedReader br = new BufferedReader(fr);
            try {
                String line;
                line = br.readLine();
                while (line != null) {
                    String[] fields = line.split("\\,");
                    String f0 = fields[0].replaceAll("\\(","").replaceAll("\\)","");
                    String f1 = fields[1].replaceAll("\\(","").replaceAll("\\)","");
                    Long src = Long.parseLong(f0);
                    Long trg = Long.parseLong(f1);
                    if (T.containsKey(src)) {
                        List<Long> p = T.get(src);
                        if(!p.contains((long) i))
                        {p.add((long) i);
                            T.put(src, p);}

                    }
                    if (T.containsKey(trg)) {
                        List<Long> p = T.get(trg);
                        if(!p.contains((long) i))
                        {p.add((long) i);
                            T.put(trg, p);}

                    }
                    // be sure to read the next line otherwise you'll get an infinite loop
                    line = br.readLine();
                }
            } finally {
                // you should close out the BufferedReader
                br.close();
            }
        }



        long sum=0;
        double rep = 0.0;

        for (Long key : T.keySet()) {

            sum=sum+T.get(key).size();
        }

        rep = (double) sum/T.size();
        System.out.println("Replication Factor:" + rep);

        FileWriter fw = new FileWriter(outputPath, true); //the true will append the new data
        fw.write(InputPath + "," +String.valueOf(rep));//appends the string to the file
        fw.write("\n");
        fw.close();
        System.out.println("Parallelism: " + env.getParallelism());



    }

    private static String InputPath = null;
    private static String outputPath = null;
    private static int k = 0;


    private static boolean parseParameters(String[] args) {

        if (args.length > 0) {
            if (args.length != 3) {
                System.err.println("<partitionedEdgesPath> <output path> <k> ");
                return false;
            }

            InputPath = args[0];
            outputPath = args[1];
            k = (int) Long.parseLong(args[2]);

        } else {
            System.out.println("Executing example with default parameters and built-in default data.");
            System.out.println("  Provide parameters to read input data from files.");
            System.out.println(" Usage: VertexCutImpl <partitionedEdgesPath> <output path> <k>");
        }
        return true;
    }


}

