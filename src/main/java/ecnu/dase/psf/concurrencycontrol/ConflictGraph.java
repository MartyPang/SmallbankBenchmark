package ecnu.dase.psf.concurrencycontrol;

import ecnu.dase.psf.common.Vertex;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ConflictGraph {
    private Map<Integer, Vertex> vertices;
    private int edgeCount; // number of edges in graph

    public ConflictGraph() {
        vertices = new HashMap<>();
        edgeCount = 0;
    }

    public void addVertex(int tranId) {
        vertices.put(tranId, new Vertex(tranId));
    }

    public boolean addEdge(int fromId, int toId, double weight) {
        boolean result = false;
        Vertex fromV = vertices.get(fromId); // Get start vertex
        Vertex toV = vertices.get(toId); // Get end vertex

        if(null != fromV && null != toV) {
            fromV.connect(toV, weight);
        }
        if(result) {
            ++edgeCount;
        }
        return result;
    }

    public boolean addEdge(int fromId, int toId) {
        return addEdge(fromId, toId, 0);
    }

    public void removeVertex(int vId) {
        Iterator<Vertex> it = vertices.values().iterator();
        while(it.hasNext()) {
            Vertex next = it.next();
            if(next.getvId_() != vId) { //remove incoming edges of vId
                next.removeNeighborById(vId);
            }
        }
        vertices.remove(vId); // remove vId itself
    }



    public Map<Integer, Vertex> getVertices() {
        return vertices;
    }

    public void setVertices(Map<Integer, Vertex> vertices) {
        this.vertices = vertices;
    }
}
