package org.myorg.quickstart.deprecated.old;

import org.myorg.quickstart.deprecated.VertexDepr;

import java.util.ArrayList;

public class EdgeDepr {

    private final VertexDepr originVertexDepr;
    private final VertexDepr destinVertexDepr;

    EdgeDepr(final VertexDepr originVertexDepr, final VertexDepr destinVertexDepr) {
        this.destinVertexDepr = destinVertexDepr;
        this.originVertexDepr = originVertexDepr;
    }

    public ArrayList<VertexDepr> getVertices() {
        ArrayList<VertexDepr> vertices = new ArrayList<>();
        vertices.add(originVertexDepr);
        vertices.add(destinVertexDepr);
        return vertices;
    }

    public VertexDepr getDestinVertexDepr() {
        return this.destinVertexDepr;
    }

    public VertexDepr getOriginVertexDepr() {
        return this.originVertexDepr;
    }


    @Override
    public String toString() {
        return "EdgeDepr{" +
                "originVertexDepr=" + originVertexDepr +
                ", destinVertexDepr=" + destinVertexDepr +
                '}';
    }
}