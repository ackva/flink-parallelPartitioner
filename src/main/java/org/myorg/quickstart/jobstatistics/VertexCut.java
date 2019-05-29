package org.myorg.quickstart.jobstatistics;

/*import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;*/

import java.io.*;
        import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by zainababbas on 21/02/2017.
 */
public class VertexCut {

    private HashMap<Long, List<Long>> vertexMap = new HashMap<>();
    private double rep = 0.0;
    private int k;

    public VertexCut(int k) {
        this.k = k;
    }

    public double calculateVertexCut(List<File> fileList) throws IOException {

        HashMap<Long, List<Long>> T = new HashMap<>();

        // Read every file inside folder (k = k = nr of files)

        for (int i = 0; i <= k; i++) {
            FileReader fr=new FileReader(fileList.get(i));
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
                    // put every distinct vertex into a hash map
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

        // Read all files again --> maintain hash map with a counter of the vertices, e.g. V 123 --> 100 times
        for (int i = 0; i <= k; i++) {
            //2. URI of the file to be read
            FileReader fr=new FileReader(fileList.get(i));
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


        // Do final calculations

        long sum=0;
        double rep = 0.0;

        for (Long key : T.keySet()) {
            sum=sum+T.get(key).size();
        }

        // According to formula:
        //       replication factor = ( # of edges cut by partitions / all edges )

        return (double) sum/T.size();

    }

    public void writeReplicationFactorToFile(int parallelism, String outputPath) throws IOException {
        FileWriter fw = new FileWriter(outputPath, true); //the true will append the new data
        fw.write("Replication"+String.valueOf(rep));//appends the string to the file
        fw.write("\n");
        fw.close();
        System.out.println("Parallelism: " + parallelism);

    }


}

