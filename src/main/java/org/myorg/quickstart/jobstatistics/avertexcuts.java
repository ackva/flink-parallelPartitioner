/*
package org.myorg.quickstart.jobstatistics;

import org.apache.flink.api.java.ExecutionEnvironment;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

*/
/**
 * Created by zainababbas on 21/02/2017.
 *//*

public class avertexcuts {

    public double calculateReplicationFactor(String inputPath, int k) throws Exception {
        HashMap<Long, List<Long>> T = new HashMap<>();
        for (int i = 1; i <= k; i++) {

            Path path = new Path(inputPath);
            BufferedReader br = new BufferedReader(in));
            try {
                String line;
                line = br.readLine();
                while (line != null) {
                    String[] fields = line.split("\\,");
                    Long src = Long.parseLong(fields[0]);
                    Long trg = Long.parseLong(fields[1]);
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
            Configuration configuration = new Configuration();
            //2. URI of the file to be read
            URI uri = new URI(InputPath+"/"+ i);
            //3. Get the instance of the HDFS
            FileSystem hdfs = FileSystem.get(uri, configuration);
            //4. A reference to hold the InputStream
            Path path = new Path(uri);
            BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(path)));
            try {
                String line;
                line = br.readLine();
                while (line != null) {
                    String[] fields = line.split("\\,");
                    Long src = Long.parseLong(fields[0]);
                    Long trg = Long.parseLong(fields[1]);
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
        System.out.println("replication factor:"+rep);

    }


}

*/
