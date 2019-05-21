package org.myorg.quickstart.deprecated.test;

import org.myorg.quickstart.jobstatistics.JobStatistics;
import org.myorg.quickstart.jobstatistics.JobSubtask;

public class JobStatisticsImpl {

    public static void main(String[] args) throws Exception {
        String jobId = "e39491c7724a9a52339e802f4e20a654";
        JobStatistics job = new JobStatistics(jobId);
        job.populateJobAttributes();
        job.getName();
        job.getParallelism();
        getJobStatistics(jobId);
    }

    public static void getJobStatistics(String jobId) throws Exception {
        JobStatistics jobStats = new JobStatistics(jobId);
        jobStats.populateJobAttributes();

        System.out.println("Some statistics");
        System.out.println("Job Name: " + jobStats.getName());
        System.out.println("Job ID: " + jobStats.getJobId());
        System.out.println("Job StateDepr: " + jobStats.getState());
        System.out.println("Job Input: " + jobStats.getInputFile());
        System.out.println("Job Parallelism: " + jobStats.getParallelism());
        System.out.println("Job Start Time: " + jobStats.getStarttime());
        System.out.println("Job Duration: " + jobStats.getDuration());
        System.out.println("-- About the load:");
        System.out.println("??? -- " + jobStats.getVertices().get(1).getName());
        for (JobSubtask sub : jobStats.getVertices().get(1).getSubtasks()) {
            System.out.println(sub.getSubtaskId());
            System.out.println(sub.getHost() + " processed " + sub.getWriteRecords() + " records");
        }
    }
}