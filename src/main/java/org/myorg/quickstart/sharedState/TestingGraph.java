package org.myorg.quickstart.sharedState;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TestingGraph {

private List<EdgeSimple> edges;

    public TestingGraph() {}

    public List<EdgeSimple> getEdges() {
        return edges;
    }

    public void generateGraphOneToAny(int numbEdges) {
        List<EdgeSimple> edgeList = new ArrayList<>();
        for (int i = 1; i < numbEdges+1; i++) {
            edgeList.add(new EdgeSimple(1,i+1));
        }
        this.edges = edgeList;
    }

    public void generateGraphOneTwoToAny(int numbEdges) {
        List<EdgeSimple> edgeList = new ArrayList<>();
        for (int i = 1; i < numbEdges/2+1; i++) {
            edgeList.add(new EdgeSimple(1,i+1));
        }
        for (int i = numbEdges/2+1; i < numbEdges+1; i++) {
            edgeList.add(new EdgeSimple(2,i+1));
        }
        this.edges = edgeList;
    }

    public void generateGraphTenRandomRemainder(int numbEdges, int remainder) {

        List<EdgeSimple> edgeList = new ArrayList<>();
        for (int i = 0; i < numbEdges; i++) {
            edgeList.add(new EdgeSimple(1,i%remainder + 2));
        }
        this.edges = edgeList;
    }

    public void generateRandomGraph(int numbEdges) {
        List<EdgeSimple> edgeList = new ArrayList<>();
        Random rand = new Random();
        for (int i = 0; i < numbEdges; i++) {
            edgeList.add(new EdgeSimple(rand.nextInt(numbEdges+1),numbEdges+1));
        }
        this.edges = edgeList;
    }

    public void printGraph() {
        System.out.println("This is the graph with its edges: ");
        for (EdgeSimple e: this.edges) {
            System.out.print(e.getOriginVertex() + "," + e.getDestinVertex() + " || ");
        }
        System.out.println();
        System.out.println("Total number of edges: " + this.edges.size());
        System.out.println("------------------");
    }

}




