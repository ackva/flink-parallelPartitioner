package org.myorg.quickstart;

import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static java.nio.file.Files.readAllLines;

/**
 *
 * #### NOT TESTED ############
 *
 *
 * This small tool compares the output of hashmaps which are stored in the following format.
 * It works until parallelism 4, but can be extended to 8 with little effort
 * FILENAME -- Choose two jobs - adjust the variables below according to their output file name (e.g. job_2000_01_01-12:12_12_log.txt and job_2015_02_08-14:01:01_log.txt)
 *
 */

public class CompareIntermediateHash {

    public static void main(String[] args) throws Exception {

        // Set File Names (MANUALLY!)
        List<String> filenames = new ArrayList<>();
        //filenames.add("output/job_2019.02.08-14.32.08_log.txt");
        //filenames.add("output/job_2019.02.08-14.32.39_log.txt");
        filenames.add("output/job_2019.02.08-12.40.36_log.txt");
        filenames.add("output/job_2019.02.08-12.39.10_log.txt");

        // Read files into List
        List<List<String>> files = new ArrayList<>(filenames.size());
        Path[] paths = new Path[filenames.size()];
        for (int i = 0; i < filenames.size(); i++) {
            paths[i] = Paths.get(filenames.get(i));
            files.add(i, readAllLines(paths[i], StandardCharsets.UTF_8));
            //System.out.println(files.get(i));
        }

        int linesInFile = files.get(0).size();

        String row = "";
        String key = "";
        int value = 0;

        for (List e:files) {
            for (int z = 0; z < e.size(); z++) {
                row = e.get(z).toString();
                row = row
                        .replaceAll("\\{", "")
                        .replaceAll("\\}", "")
                        .replaceAll("\\;", "")
                        .replaceAll("\\s+", "");
                //System.out.println(row);
            }
            System.out.println("nextFile");
        }

        //String test = files.get(0).get(1200).substring(0,50);

        //int intersection = files.get(0).get(1200).retainAll()

        String currRow;
        String nextRow;

        for (int i = (int) 0.95 * linesInFile; i < linesInFile; i++) {
            //System.out.println(files.get(0).get(i));
            currRow = files.get(0).get(i).substring(0, 5)     + files.get(0).get(linesInFile / 10).substring(0, 5)     + files.get(0).get(linesInFile / 20).substring(0, 5)     + files.get(0).get(linesInFile / 50).substring(0, 5);
            nextRow = files.get(0).get(i - 1).substring(0, 5) + files.get(0).get(linesInFile / 10 - 1).substring(0, 5) + files.get(0).get(linesInFile / 20 - 1).substring(0, 5) + files.get(0).get(linesInFile / 50 - 1).substring(0, 5);

        }

        // Parse Strings into HashMaps

/*
        // "Re-build" parallelism 1 hashMap
        String[] a = new String[2];
        String[] tokensParallelOne = parallelOne.split("\\,");
        for (String s : tokensParallelOne) {
            key = s.substring(0, s.indexOf("="));
            value = Integer.parseInt(s.substring(s.indexOf("=") + 1, s.length()));
            tableWithParallelOne.put(key, value);

*/


        /*// Loop over all lines of the input file
        for (int i = 0; i < allLines.size(); i++) {
            // Parallelism = 1
            if (allLines.get(i).contains("parallelism 1")) {
                parallelOne = allLines.get(i + 1)
                        .replaceAll("\\{", "")
                        .replaceAll("\\}", "")
                        .replaceAll("\\s+", "");
                i++;
            }*/

    }
}
