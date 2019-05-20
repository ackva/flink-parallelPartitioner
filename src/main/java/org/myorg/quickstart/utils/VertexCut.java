package org.myorg.quickstart.utils;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
public class VertexCut {

    private HashMap<Long, List<Long>> vertexMap = new HashMap<>();
    private double rep = 0.0;
    private int parallelism;

    public VertexCut(int parallelism) {
        this.parallelism = parallelism;
    }

    public double calculateVertexCut(List<File> fileList) throws IOException {

        for (File f : fileList) {
            FileReader fr = new FileReader(f);
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
                    if (!vertexMap.containsKey(src)) {
                        vertexMap.put(src, new ArrayList<>());
                    }
                    if (!vertexMap.containsKey(trg)) {
                        vertexMap.put(trg, new ArrayList<>());
                    }
                    // be sure to read the next line otherwise you'll get an infinite loop
                    line = br.readLine();
                }
            } finally {
                // you should close out the BufferedReader
                br.close();
            }
        }


        for (int i = 1; i <= parallelism; i++) {
            //2. URI of the file to be read
            FileReader fr = new FileReader(fileList.get(i));
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
                    if (vertexMap.containsKey(src)) {
                        List<Long> p = vertexMap.get(src);
                        if(!p.contains((long) i))
                        {p.add((long) i);
                            vertexMap.put(src, p);}

                    }
                    if (vertexMap.containsKey(trg)) {
                        List<Long> p = vertexMap.get(trg);
                        if(!p.contains((long) i))
                        {p.add((long) i);
                            vertexMap.put(trg, p);}

                    }
                    // be sure to read the next line otherwise you'll get an infinite loop
                    line = br.readLine();
                }
            } finally {
                // you should close out the BufferedReader
                br.close();
            }
        }

        long sum = 0;
        rep = 0.0;

        for (Long key : vertexMap.keySet()) {

            sum = sum + vertexMap.get(key).size();
        }

        rep = (double) sum / vertexMap.size();
        return rep;

    }

    public void writeReplicationFactorToFile(int parallelism, String outputPath) throws IOException {
        FileWriter fw = new FileWriter(outputPath, true); //the true will append the new data
        fw.write("Replication"+String.valueOf(rep));//appends the string to the file
        fw.write("\n");
        fw.close();
        System.out.println("Parallelism: " + parallelism);

    }


}

