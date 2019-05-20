package org.myorg.quickstart.jobstatistics;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 *
 * Verifies whether the results of different "test" runs for initial partitioning are same, or different.
 * ### first test passed - adjustment needed to search log directory instead of manually created "merged.txt" file
 *
 */

public class CompareLog {


    public static void main(String[] args) throws Exception {

        // Read File
        String fileName = "input/merged140219-1054.txt";
        Path path = Paths.get(fileName);
        //byte[] bytes = Files.readAllBytes(path);
        List<String> allLines = Files.readAllLines(path, StandardCharsets.UTF_8);

        //ArrayList jobs = new ArrayList();
            HashMap<String, String > jobs = new HashMap();
            HashMap partitions = new HashMap();
            String key = "";
            int worker = -1;
            ArrayList<Integer> jobCounter = new ArrayList();

            String partitionPrint = "";
        JaccardSimilarity js = new JaccardSimilarity();


        for (int i = 0; i < allLines.size(); i++) {
            // regex: \d{4}.\d{2}.\d{2}-\d{2}.\d{2}.\d{2}:\d> for 2019.02.11-12.26.52:2>
            if (allLines.get(i).matches("\\d{4}.\\d{2}.\\d{2}-\\d{2}.\\d{2}.\\d{2}(.*)")) {

                String[] splitLine = allLines.get(i).split(">", 2);
                String[] splitValue = splitLine[1].split("\\(", 2);

                // Key separation
                if (splitLine[0].length() > 19) {
                    key = splitLine[0].substring(0,21);
                    worker = Integer.parseInt(splitLine[0].substring(20,21));
                } else {
                    key = splitLine[0].substring(0,19);
                    worker = 0;
                }

                partitionPrint = splitValue[1].replaceAll("\\}|\\{|\\)", "");

                // Put hash table into jobs map
                if (! jobs.keySet().contains(key)) {
                    if (i < allLines.size()-1) {
                        if (allLines.get(i + 1).matches("\\d{4}.\\d{2}.\\d{2}-\\d{2}.\\d{2}.\\d{2}(.*)")) {
                            if (allLines.get(i + 1).contains(splitLine[0])) {
                                i++;
                            } else {
                                jobs.put(key, partitionPrint);
                            }
                        } else {
                            i++;
                        }
                    }
                } else {
                        jobs.put(key, "abc");
                }
            }
        }

        System.out.println("keys: " + jobs.keySet());

        Object[] keys = jobs.keySet().toArray();
        Object[] values = jobs.values().toArray();
        Arrays.sort(keys);
        Arrays.sort(values);

        for (int i = 0; i < keys.length - 1; i++) {
            System.out.println(keys[i] + " key -- value:  " + values[i]);
            System.out.println(keys[i+1] + " key -- value:  " + values[i+1]);
            if (values[i].toString().equals(values[i + 1].toString()) == true) {
                System.out.println("Similarity for keys " + keys[i] + " and " + keys[i + 1]);
            } else {
                System.out.println("No similarity for keys " + keys[i] + " and " + keys[i + 1]);
            }
            System.out.println("NEXT ROUND");
        }


        // System.out.println("Klappt?");

        for (Object v : values) {
            System.out.println("value: " + v);
        }

    }
}