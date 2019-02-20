package org.myorg.quickstart.jobstatistics;

import java.util.Date;

public class JobSubtask {
    private String subtaskId;
    private String vertexId;
    private String jobId;
    private String status;
    private String startTime;
    private String host;
    private long duration;
    private long readBytes;
    private long writeBytes;
    private long readRecords;
    private long writeRecords;

    public JobSubtask(String subtaskId, String vertexId, String jobId) {
        this.subtaskId = subtaskId;
        this.vertexId = vertexId;
        this.jobId = jobId;
    }

    public String getSubtaskId() {
        return subtaskId;
    }

    public void setSubtaskId(String subtaskId) {
        this.subtaskId = subtaskId;
    }

    public String getVertexId() {
        return vertexId;
    }

    public void setVertexId(String vertexId) {
        this.vertexId = vertexId;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public long getReadBytes() {
        return readBytes;
    }

    public void setReadBytes(long readBytes) {
        this.readBytes = readBytes;
    }

    public long getWriteBytes() {
        return writeBytes;
    }

    public void setWriteBytes(long writeBytes) {
        this.writeBytes = writeBytes;
    }

    public long getReadRecords() {
        return readRecords;
    }

    public void setReadRecords(long readRecords) {
        this.readRecords = readRecords;
    }

    public long getWriteRecords() {
        return writeRecords;
    }

    public void setWriteRecords(long writeRecords) {
        this.writeRecords = writeRecords;
    }

}
