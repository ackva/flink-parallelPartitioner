package org.myorg.quickstart.ForGelly;

import org.apache.flink.graph.Edge;

import java.text.SimpleDateFormat;
import java.util.Date;

public class EdgeEventGelly {

    private Edge edge;
    private Long eventTime;

    public EdgeEventGelly(Edge edge) {
        this.edge = edge;
        this.eventTime = System.currentTimeMillis();
    }

    public Edge getEdge() {
        return this.edge;
    }

    public long getEventTime() {
        return this.eventTime;
    }

    public void printEdgeEvent() {
        System.out.println(this.edge.f0 + " " + this.edge.f1 + " "
                + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(this.getEventTime())));
    }
}
