package org.myorg.quickstart.jobstatistics;

import java.io.IOException;

public class StandaloneStatistics {

    public static void main(String[] args) throws IOException {
        VertexCutsMultipleJobs vertexCuts = new VertexCutsMultipleJobs(args[0]);
        vertexCuts.calculate();


    }
}
