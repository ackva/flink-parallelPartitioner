package org.myorg.quickstart.deprecated.old;

import org.myorg.quickstart.deprecated.Vertex;

import java.util.ArrayList;

public class Edge {

    private final Vertex originVertex;
    private final Vertex destinVertex;

    Edge(final Vertex originVertex, final Vertex destinVertex) {
        this.destinVertex = destinVertex;
        this.originVertex = originVertex;
    }

    public ArrayList<Vertex> getVertices() {
        ArrayList<Vertex> vertices = new ArrayList<>();
        vertices.add(originVertex);
        vertices.add(destinVertex);
        return vertices;
    }

    public Vertex getDestinVertex() {
        return this.destinVertex;
    }

    public Vertex getOriginVertex() {
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