package org.myorg.quickstart;

import org.myorg.quickstart.utils.JaccardSimilarity;

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
        String fileName = "input/merged.txt";
        Path path = Paths.get(fileName);
        byte[] bytes = Files.readAllBytes(path);
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
                    //System.out.println("key: " + key + " --- worker: " + worker);

                // Put hash table into jobs map
                if (! jobs.keySet().contains(key)) {
                    if (i < allLines.size()-2) {
                        if (allLines.get(i + 2).matches("\\d{4}.\\d{2}.\\d{2}-\\d{2}.\\d{2}.\\d{2}(.*)")) {
                            if (allLines.get(i + 2).contains(splitLine[0])) {
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
                /*else {
                        partitionPrint = partitionPrint
                    }

                    jobs.put(key,partitionPrint);
                }*/


//                partitionPrint = splitLine[1];
                //System.out.println("This is the key: " + key);
            }


        }

        System.out.println("keys: " + jobs.keySet());

        Object[] keys = jobs.keySet().toArray();
        Object[] values = jobs.values().toArray();
        Arrays.sort(keys);
        Arrays.sort(values);

        for (int i = 0; i < keys.length - 1; i++) {
            System.out.println(keys[i]);
            System.out.println(values[i]);
            if (values[i].toString().equals(values[i + 1].toString()) == true) {
                System.out.println("Similarity for keys " + keys[i] + " and " + keys[i + 1]);
            } else {
                System.out.println("No similarity for keys " + keys[i] + " and " + keys[i + 1]);
            }
            System.out.println("NEXT ROUND");
        }


        System.out.println("Klappt?");

        for (Object v : values) {
            System.out.println("value: " + v);
        }


        //System.out.println("Value: " + jobs.get(k));


    }
    }


        /*
        // Declare and initialize some variables
        HashMap tableWithParallelOne = new HashMap();
        HashMap tableWithParallelTwo = new HashMap();
        HashMap tableWithParallelFour = new HashMap();
        String parallelOne = "";
        String parallelTwo = "";
        String parallelFour = "";
        int parallelism = 0;


        //// Parse all partitions into Strings (again)

        // Loop over all lines of the input file
        for (int i = 0; i < allLines.size(); i++) {
            // Parallelism = 1
            if (allLines.get(i).contains("parallelism 1")) {
                parallelOne = allLines.get(i + 1)
                        .replaceAll("\\{", "")
                        .replaceAll("\\}", "")
                        .replaceAll("\\s+", "");
                i++;
            }
            // Parallelism = 2
            if (allLines.get(i).contains("parallelism 2")) {
                parallelTwo = allLines.get(i + 2)
                        .replaceAll("\\{", "")
                        .replaceAll("\\}", "")
                        .replaceAll("\\s+", "");
                parallelTwo = parallelTwo + "," + allLines.get(i + 4)
                        .replaceAll("\\{", "")
                        .replaceAll("\\}", "")
                        .replaceAll("\\s+", "");
                i++;
                parallelism = 2;
            }
            // Parallelism = 4
            if (allLines.get(i).contains("parallelism 4")) {
                parallelFour = allLines.get(i + 2)
                        .replaceAll("\\{", "")
                        .replaceAll("\\}", "")
                        .replaceAll("\\s+", "");
                parallelFour = parallelFour + "," + allLines.get(i + 4)
                        .replaceAll("\\{", "")
                        .replaceAll("\\}", "")
                        .replaceAll("\\s+", "");
                parallelFour = parallelFour + "," + allLines.get(i + 6)
                        .replaceAll("\\{", "")
                        .replaceAll("\\}", "")
                        .replaceAll("\\s+", "");
                parallelFour = parallelFour + "," + allLines.get(i + 8)
                        .replaceAll("\\{", "")
                        .replaceAll("\\}", "")
                        .replaceAll("\\s+", "");
                i++;
                parallelism = 4;
            }
        }


*//*      System.out.println("Parallel One-- " + parallelOne);
        System.out.println("Parallel Two-- " + parallelTwo);
        System.out.println("Parallel Four-- " + parallelFour);
        System.out.println("Parallelism = " + parallelism);*//*

        // Parse Strings into HashMaps
        String key = "";
        int value = 0;

        // "Re-build" parallelism 1 hashMap
        String[] a = new String[2];
        String[] tokensParallelOne = parallelOne.split("\\,");
        for (String s : tokensParallelOne) {
            key = s.substring(0, s.indexOf("="));
            value = Integer.parseInt(s.substring(s.indexOf("=") + 1, s.length()));
            tableWithParallelOne.put(key, value);
        }
        //System.out.println("HashMap p = 1: " + tableWithParallelOne);

        // "Re-build" parallelism 2 hashMap
        int count = 0;
        Arrays.fill(a, null);
        String[] tokensParallelTwo = parallelTwo.split("\\,");
        for (String s : tokensParallelTwo) {
            key = s.substring(0, s.indexOf("="));
            value = Integer.parseInt(s.substring(s.indexOf("=") + 1, s.length()));
            // Check if key is already in map (from first partition) -- if so, increase the value
            if (tableWithParallelTwo.containsKey(key)) {
                count = Integer.parseInt(tableWithParallelTwo.get(key).toString());
                count = count + value;
            } else {
                count = value;
            }
            tableWithParallelTwo.put(key, count);
        }
        //System.out.println("HashMap p = 2: " + tableWithParallelTwo);


        // "Re-build" parallelism 4 hashMap
        if (parallelism == 4) {
            Arrays.fill(a, null);
            String[] tokensParallelFour = parallelFour.split("\\,");
            for (String s : tokensParallelFour) {
                key = s.substring(0, s.indexOf("="));
                value = Integer.parseInt(s.substring(s.indexOf("=") + 1, s.length()));
                // Check if key is already in map (from first three partitions) -- if so, increase the value
                if (tableWithParallelFour.containsKey(key)) {
                    count = Integer.parseInt(tableWithParallelFour.get(key).toString());
                    count = count + value;
                } else {
                    count = value;
                }
                tableWithParallelFour.put(key, count);
            }
        }
        //System.out.println("HashMap p = 4: " + tableWithParallelFour);


        //// Get Results - i.e. compare
        if (parallelism == 4) {
            if (tableWithParallelOne.equals(tableWithParallelTwo)) {
                if (tableWithParallelOne.equals(tableWithParallelFour)) {
                    System.out.println("All hash maps result in the same! EQUAL!");
                } else {
                    System.out.println("The tables are not equal (parallel 1 != parallel 4)");
                }
            }
        } else if (parallelism == 2) {
            if (tableWithParallelOne.equals(tableWithParallelTwo)) {
                System.out.println("All hash maps result in the same! EQUAL!");
            } else {
                System.out.println("The tables are not equal (parallel 1 != parallel 2)");
            }
        } else {
            System.out.println("Parallelism isn't 2 or 4. Please check input");
        }*/

