package org.myorg.quickstart.jobstatistics;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 *
 *
 * #### NOT TESTED ############
 *
 * This small tool compares the output of hashmaps which are stored in the following format.
 * It works until parallelism 4, but can be extended to 8 with little effort
 * FILENAME -- inputComparison.txt
 *
 * It must looks as follows (hardcoded, super static!)
 *
 * parallelism 1:
 * {1220=4, 1462=2, 1461=2, 1541=1, 253=4}
 * parallelism 2:
 * Worker 1:
 * {1220=3, 1462=1, 1461=1, 253=2}
 * Worker 2:
 * {1220=1, 1462=1, 253=2, 1461=1, 1541=1}
 * parallelism 4:
 * Worker 1:
 * {1220=1, 1461=1, 253=2}
 * Worker 2:
 * {253=2, 1541=1}
 * Worker 3:
 * {1220=2, 1462=1}
 * Worker 4:
 * {1220=1, 1462=1, 1461=1}
 *
 */

public class CompareResult {

    public static void main(String[] args) throws Exception {

        // Read File
        String fileName = "files/inputComparison.txt";
        Path path = Paths.get(fileName);
        byte[] bytes = Files.readAllBytes(path);
        List<String> allLines = Files.readAllLines(path, StandardCharsets.UTF_8);

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


/*      System.out.println("Parallel One-- " + parallelOne);
        System.out.println("Parallel Two-- " + parallelTwo);
        System.out.println("Parallel Four-- " + parallelFour);
        System.out.println("Parallelism = " + parallelism);*/

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
        }
    }
}
