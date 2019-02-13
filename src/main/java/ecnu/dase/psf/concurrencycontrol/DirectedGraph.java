package ecnu.dase.psf.concurrencycontrol;

import ecnu.dase.psf.common.Vertex;

import java.util.*;

public class DirectedGraph {
    private Map<Integer, Vertex> vertices;
    private int edgeCount; // number of edges in graph

    public DirectedGraph() {
        vertices = new HashMap<>();
        edgeCount = 0;
    }

    public DirectedGraph(Map<Integer, Vertex> dg) {
        vertices = dg;
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
            //remove incoming edges of vId
            if(next.getvId_() != vId) {
                next.removeNeighborById(vId);
            }
            //remove outgoing edges of vId
            else {
                Iterator<Vertex> neighbors = next.getNeighborIterator();
                while(neighbors.hasNext()) {
                    //Decrease in-degree by one
                    neighbors.next().decreaseDegreeByOne(true);
                }
            }
        }
        // remove vId itself
        vertices.remove(vId);
    }

    /**
     * Trim vertex with no incoming/outgoing edges recursively.
     */
    public void trimGraph() {
        Vertex trimmedV = getNextTrimVertex();
        if(trimmedV != null) {
            removeVertex(trimmedV.getvId_());
            System.out.printf("Trim vertex %d\n", trimmedV.getvId_());
            trimGraph();
        }
    }

    private Vertex getNextTrimVertex() {
        Vertex result = null;
        Iterator<Vertex> it = vertices.values().iterator();
        boolean found = false;
        while(!found && it.hasNext()) {
            Vertex vertex = it.next();
            if(vertex.getOutDegree() == 0 || vertex.getInDegree() == 0) {
                result = vertex;
                found = true;
            }
        }
        return result;
    }

    /**
     * 获取图的拓扑排序
     * 每次选择一个出度为0的节点加入到栈中
     * 从栈依次pop出节点即可得到拓扑序
     * @return
     */
    public Stack<Integer> getTopologicalSort() {
        //set all vertices to unvisited
        resetGraph();
        Stack<Integer> vertexStack = new Stack<>();
        int verticesCount = vertices.size();

        for(int i = 0; i < verticesCount; ++i) {
            Vertex next = getNextTopologyVertex();
            if(null != next) {
                next.visit();
                vertexStack.push(next.getvId_());
            }
        }
        return  vertexStack;
    }

    /**
     * 获取一个出度为0的节点
     * @return
     */
    private Vertex getNextTopologyVertex() {
        Vertex vertex = null;
        Iterator<Vertex> it = vertices.values().iterator();
        boolean found = false;
        while(!found && it.hasNext()) {
            vertex = it.next();
            if(!vertex.isVisited() && vertex.getUnvisitedNeighbor() == null) {
                found = true;
            }
        }
        return vertex;
    }

    public Collection<Integer> getVertexIdSet() {
        return vertices.keySet();
    }

    public int getGraphSize() {
        return vertices.size();
    }

    public Map<Integer, Vertex> getVertices() {
        return vertices;
    }

    public void setVertices(Map<Integer, Vertex> vertices) {
        this.vertices = vertices;
    }

    public void printGraph() {
        Collection<Vertex> vertexSet = vertices.values();
        for(Vertex v : vertexSet) {
            v.printVertex();
        }
    }

    public void resetGraph() {
        Iterator<Vertex> it = vertices.values().iterator();
        while(it.hasNext()) {
            Vertex v = it.next();
            v.unVisit();
        }
    }

    public int getEdgeCount() {
        return edgeCount;
    }

    public void setEdgeCount(int edgeCount) {
        this.edgeCount = edgeCount;
    }
}
