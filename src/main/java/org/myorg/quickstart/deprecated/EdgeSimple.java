package org.myorg.quickstart.deprecated;

import java.util.ArrayList;

public class EdgeSimple {

    private final int originVertex;
    private final int destinVertex;

    EdgeSimple(final int originVertex, final int destinVertex) {
        this.destinVertex = destinVertex;
        this.originVertex = originVertex;
    }

    public ArrayList<Integer> getVertices() {
        ArrayList<Integer> vertices = new ArrayList<>();
        vertices.add(originVertex);
        vertices.add(destinVertex);
        return vertices;
    }

    public int getDestinVertex() {
        return this.destinVertex;
    }

    public int getOriginVertex() {
        return this.originVertex;
    }

    @Override
    public String toString() {
        return "Edge{" +
                "originVertex=" + originVertex +
                ", destinVertex=" + destinVertex +
                '}';
    }
}