package org.myorg.quickstart.jobstatistics;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.List;

public class LoadBalanceCalculator {

    public LoadBalanceCalculator() {
    }

    public double calculateLoad(List<File> fileList) throws IOException {
        int fileCounter = 0;
        int totalNumOfEdges = 0;
        int highestLoad = 0;
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
                    System.out.println(numberOfLines);
                }
            totalNumOfEdges = totalNumOfEdges + numberOfLines;

            // Increase Highest Load, if applicable
            if (numberOfLines > highestLoad)
                highestLoad = numberOfLines;
        }

        double load = highestLoad / (totalNumOfEdges / fileCounter);

        return load;
    }
}
