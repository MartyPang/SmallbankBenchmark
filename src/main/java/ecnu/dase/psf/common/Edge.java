package ecnu.dase.psf.common;

import org.jetbrains.annotations.NotNull;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Edge implements Comparable<Edge> {
    private Vertex startVertex;
    private Vertex endVertex;
    private int weight;
    private Map<String, Item> consistentR; // Consistent read set

    public Edge(Vertex startVertex, Vertex endVertex, int weight) {
        this.startVertex = startVertex;
        this.endVertex = endVertex;
        this.weight = weight;
        consistentR = new HashMap<>();
    }

    public void updateConsistentReadset(String key, Item value) {
        consistentR.putIfAbsent(key, value);
        weight += key.length();
        weight += 16; //Item.value
        weight += 16; //Item.getWrittenBy
    }

    public Vertex getStartVertex() {
        return startVertex;
    }

    public void setStartVertex(Vertex startVertex) {
        this.startVertex = startVertex;
    }

    public Vertex getEndVertex() {
        return endVertex;
    }

    public void setEndVertex(Vertex endVertex) {
        this.endVertex = endVertex;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    public Map<String, Item> getConsistentR() {
        return consistentR;
    }

    public void setConsistentR(Map<String, Item> consistentR) {
        this.consistentR = consistentR;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Edge edge = (Edge) o;
        return startVertex.getvId_() == edge.getStartVertex().getvId_() &&
                endVertex.getvId_() == edge.getEndVertex().getvId_();
    }

    @Override
    public int hashCode() {
        return Objects.hash(startVertex, endVertex);
    }

    @Override
    public int compareTo(@NotNull Edge e) {
        int result = 0;
        if(weight > e.getWeight()) {
            result = 1;
        }
        else if(weight < e.getWeight()) {
            result = -1;
        }
        return result;
    }
}
