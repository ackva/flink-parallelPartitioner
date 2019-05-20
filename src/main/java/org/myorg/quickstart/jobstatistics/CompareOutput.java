package org.myorg.quickstart.jobstatistics;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 *
 *
 * #### NOT Really useful atm ############
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

public class CompareOutput {

    private String fileName;
    private int parallelism;
    private String timeStamp;

    public CompareOutput(String fileName, int parallelism, String timeStamp) {
        this.fileName = fileName;
        this.parallelism = parallelism;
        this.timeStamp = timeStamp;
    }

    public boolean checkEquality() throws java.io.IOException {

        // Read File
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

        String rowA = "";
        String rowB = "";

        //// Parse all partitions into Strings (again)

        // Loop over all lines of the input file
        for (int i = allLines.size(); i > 1; i--) {
            rowA = allLines.get(i)
                    .replaceAll("\\{", "")
                    .replaceAll("\\}", "")
                    .replaceAll("\\s+", "");
            rowB = allLines.get(i - 1)
                    .replaceAll("\\{", "")
                    .replaceAll("\\}", "")
                    .replaceAll("\\s+", "");
            if (rowA.length() / rowB.length() > 0.9) {
                if (rowA.substring(rowA.length() - 20, rowA.length() - 10) == (rowB.substring(rowB.length() - 20, rowB.length() - 10))) {
                    i--;


                } else {
                    i--;
                }

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
        return false;
    }
}



