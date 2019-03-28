package org.myorg.quickstart.sharedState;

public class Edge {

    private final Vertex originVertex;
    private final Vertex destinVertex;

    Edge(final Vertex originVertex, final Vertex destinVertex) {
        this.destinVertex = destinVertex;
        this.originVertex = originVertex;
    }

    public Vertex getVertex() {
        return destinVertex;
    }

    @Override
    public String toString() {
        return "Edge{" +
                "originVertex=" + originVertex +
                ", destinVertex=" + destinVertex +
                '}';
    }
}