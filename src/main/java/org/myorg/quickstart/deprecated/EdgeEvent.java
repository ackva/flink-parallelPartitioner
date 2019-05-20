package org.myorg.quickstart.deprecated;

import java.text.SimpleDateFormat;
import java.util.Date;

public class EdgeEvent {

    private EdgeSimple edge;
    private Long eventTime;

    public EdgeEvent(EdgeSimple edge) {
        this.edge = edge;
        this.eventTime = System.currentTimeMillis();
    }

    public EdgeSimple getEdge() {
        return this.edge;
    }

    public long getEventTime() {
        return this.eventTime;
    }

    public void printEdgeEvent() {
      System.out.println(this.edge.getOriginVertex() + " " + this.edge.getDestinVertex() + " "
                + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(this.getEventTime())));
    }
}
