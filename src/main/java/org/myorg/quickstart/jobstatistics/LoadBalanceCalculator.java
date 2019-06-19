package org.myorg.quickstart.jobstatistics;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.List;

public class LoadBalanceCalculator {

    private double totalNumberOfEdges;

    public LoadBalanceCalculator() {
    }

    public double calculateLoad(List<File> fileList) throws IOException {
        double fileCounter = 0;
        double totalNumOfEdges = 0;
        double highestLoad = 0;
        for (File f : fileList) {
            fileCounter++;
            int numberOfLines = 0;
            try (
                            FileReader input = new FileReader(f);
                            LineNumberReader count = new LineNumberReader(input)
            )
                {
                while (count.skip(Long.MAX_VALUE) > 0)
                {
                    // Loop just in case the file is > Long.MAX_VALUE or skip() decides to not read the entire file
                }

                numberOfLines = count.getLineNumber() + 1;                                    // +1 because line index starts at 0
                }
            totalNumOfEdges = totalNumOfEdges + numberOfLines;

            // Increase Highest Load, if applicable
            if (numberOfLines > highestLoad)
                highestLoad = numberOfLines;
        }

        double load = highestLoad / (totalNumOfEdges / fileCounter);

        // store in instance variable (-2 because newLine is always added to the original file)
        this.totalNumberOfEdges = totalNumOfEdges - fileCounter;

        return load;
    }

    public double getTotalNumberOfEdges() {
        return totalNumberOfEdges;
    }
}
