package org.myorg.quickstart.jobstatistics;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StandaloneSinglePartitioner {

    public static void main(String[] args) throws IOException {

        // Read directory name from arguments
        String directoryName = args[0];

        // Iterate over all folders inside directory
        File directory = new File(directoryName);
        File[] filesArray = directory.listFiles();
        //sort all files
        Arrays.sort(filesArray);
        //print the sorted values
        for (File file : filesArray) {
            if (file.isFile()) {
                System.out.println("File : " + file.getName());

            // if it's a directory with "job_" prefix, count all files and put them into a list
            } else if (file.isDirectory() && file.getName().contains("zainab")) {
                File[] jobOutputFiles = file.listFiles();
                System.out.print( file.getName() + " -- Files: ");
                List<File> fileList = new ArrayList<>();
                int parallelism = 0;
                for (File f : jobOutputFiles) {
                    fileList.add(f);
                    System.out.print(f.getName() + " ");
                    parallelism=+1;
                }
                System.out.println();

                // GET REPLICATION FACTOR with file list
                double vertexCut = new VertexCut(parallelism).calculateVertexCut(fileList);
                System.out.println("Replication Factor: " + vertexCut);

                // GET LOAD BALANCE with file list
                double loadBalance = new LoadBalanceCalculator().calculateLoad(fileList);
                System.out.println("Load Balance: " + loadBalance);
                System.out.println(" --- ");

                //print("Directory : " + file.getName());
            } else {
                System.out.println("No Job Output Directory found : " + file.getName());
            }
        }

    }
}
