package org.myorg.quickstart.sharedState;

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
}
