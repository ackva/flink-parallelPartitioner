package org.myorg.quickstart.jobstatistics;

import java.util.ArrayList;

import com.google.gson.*;

public class JobStatistics {

    private String jobId;
    private String name;
    private String state;
    private String starttime;
    private long duration;
    private String inputFile;
    private int parallelism;
    private ArrayList<JobVertex> vertices;

    public JobStatistics(String jobId) {
        this.jobId = jobId;
        this.name = "";
        this.state = "";
        this.starttime = "";
        this.duration = 0L;
        this.inputFile = "";
        this.parallelism = 0;
        this.vertices = new ArrayList<>();

    }

    public void populateJobAttributes() throws Exception {

        String vertexId;
        String subtaskId;
        ArrayList<JobSubtask> jobSubtasks = new ArrayList<>();


        // Make API calls to retrieve Job Details and Job Config
        FlinkRestApiGet apiCall = new FlinkRestApiGet();
        String jobDetails = apiCall.getJobHTML(jobId);
        String jobConfig = apiCall.getJobConfigHTML(jobId);
        /*System.out.println("Job Details");
        System.out.println(jobDetails);
        System.out.println("Job Config");
        System.out.println(jobConfig);*/

        // Use GSON to populate the attributes
        JsonParser jsonParser = new JsonParser();

        // Parse Job Details into attributes
        JsonObject jobDetailJson = jsonParser.parse(jobDetails).getAsJsonObject();
        this.name = jobDetailJson.get("name").toString().replaceAll("\"", "");
        this.state = jobDetailJson.get("state").toString().replaceAll("\"", "");
        this.starttime = jobDetailJson.get("start-time").toString().replaceAll("\"", "");
        this.duration = Long.valueOf(jobDetailJson.get("duration").toString());

        // Parse Job Config into attributes
        JsonObject jobConfigJson = jsonParser.parse(jobConfig).getAsJsonObject();
        this.inputFile = jobConfigJson
                .get("execution-config")
                .getAsJsonObject()
                .get("user-config")
                .getAsJsonObject()
                .get("input")
                .toString().replaceAll("\"", "");
        this.parallelism = Integer.parseInt(
                jobConfigJson.get("execution-config")
                .getAsJsonObject()
                .get("job-parallelism").toString());

        // Create list of Vertices that belong to this Job
        JsonArray jobVerticesJson = jobDetailJson.getAsJsonArray("vertices");

        // Parse all vertices, i.e. parts of the Flink DAG, into the JobVertex objects
        for (JsonElement o: jobVerticesJson) {
            vertexId = o.getAsJsonObject().get("id").toString().replaceAll("\"", "");
            JobVertex jv = new JobVertex(vertexId, jobId);
            JsonObject vertexJson = jsonParser.parse(apiCall.getVertexHTML(jobId, vertexId)).getAsJsonObject();

            // Get different attributes from JSON
            jv.setName(vertexJson.get("name").toString());
            jv.setParallelism(Integer.parseInt(vertexJson.get("parallelism").toString()));
            jv.setStatus(o.getAsJsonObject().get("status").toString().replaceAll("\"", ""));
            jv.setStarttime(o.getAsJsonObject().get("start-time").toString().replaceAll("\"", ""));
            jv.setDuration(o.getAsJsonObject().get("duration").getAsNumber().longValue());
            jv.setReadBytes(o.getAsJsonObject().get("metrics").getAsJsonObject().get("read-bytes").getAsNumber().longValue());
            jv.setWriteBytes(o.getAsJsonObject().get("metrics").getAsJsonObject().get("write-bytes").getAsNumber().longValue());
            jv.setReadRecords(o.getAsJsonObject().get("metrics").getAsJsonObject().get("read-records").getAsNumber().longValue());
            jv.setWriteRecords(o.getAsJsonObject().get("metrics").getAsJsonObject().get("write-records").getAsNumber().longValue());

            vertices.add(jv);
            //System.out.println(vertexId);

            JsonArray subtaskJson = vertexJson.getAsJsonArray("subtasks");

            for (JsonElement u: subtaskJson) {
                subtaskId = vertexId + "_" + u.getAsJsonObject().get("subtask").toString().replaceAll("\"", "");
                JobSubtask sub = new JobSubtask(subtaskId, vertexId, jobId);

                sub.setStatus(u.getAsJsonObject().get("status").toString().replaceAll("\"", ""));
                sub.setStartTime(u.getAsJsonObject().get("start_time").toString().replaceAll("\"", ""));
                sub.setDuration(u.getAsJsonObject().get("duration").getAsNumber().longValue());
                sub.setHost(u.getAsJsonObject().get("host").getAsString().replaceAll("\"", ""));
                sub.setReadBytes(u.getAsJsonObject().get("metrics").getAsJsonObject().get("read-bytes").getAsNumber().longValue());
                sub.setWriteBytes(u.getAsJsonObject().get("metrics").getAsJsonObject().get("write-bytes").getAsNumber().longValue());
                sub.setReadRecords(u.getAsJsonObject().get("metrics").getAsJsonObject().get("read-records").getAsNumber().longValue());
                sub.setWriteRecords(u.getAsJsonObject().get("metrics").getAsJsonObject().get("write-records").getAsNumber().longValue());
                //System.out.println(subtaskId);
                jobSubtasks.add(sub);
            }
            jv.setSubtasks(jobSubtasks);

        }

    }

    public String getJobId() {
        return jobId;
    }

    public String getName() {
        return name;
    }

    public String getState() {
        return state;
    }

    public String getStarttime() {
        return starttime;
    }

    public long getDuration() {
        return duration;
    }

    public String getInputFile() {
        return inputFile;
    }

    public int getParallelism() {
        return parallelism;
    }

    public ArrayList<JobVertex> getVertices() {
        return vertices;
    }
}
