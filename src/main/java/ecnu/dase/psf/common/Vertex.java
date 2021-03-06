package ecnu.dase.psf.common;

import java.util.*;

public class Vertex {
    private int vId_; // transaction id
    private List<Edge> edgeList_; // outgoing edge list
    private List<Integer> incomingEdge;
    private boolean visited_;
    private int weight_; // execution time
    private int inDegree;
    private int outDegree;
    private int dfNumber;
    private int lowLink;


    public Vertex(int vId_) {
        this.vId_ = vId_;
        edgeList_ = new LinkedList<>();
        incomingEdge = new ArrayList<>();
        visited_ = false;
        weight_ = 0;
        inDegree = 0;
        outDegree = 0;
        lowLink = 0;
        dfNumber = 0;
    }

    public Vertex(int vId_, int weight_) {
        this.vId_ = vId_;
        edgeList_ = new LinkedList<>();
        incomingEdge = new ArrayList<>();
        visited_ = false;
        this.weight_ = weight_;
        inDegree = 0;
        outDegree = 0;
        lowLink = 0;
        dfNumber = 0;
    }

    /**
     * Traverse its all neighbor vertex through the iterator of edgeList
     */
    private class NeighborIterator implements Iterator<Vertex> {
        Iterator<Edge> edgeIterator;
        private NeighborIterator() {
            edgeIterator = edgeList_.iterator();
        }

        @Override
        public boolean hasNext() {
            return edgeIterator.hasNext();
        }

        @Override
        public Vertex next() {
            Vertex nextNeighbor = null;
            if(edgeIterator.hasNext()) {
                Edge edge = edgeIterator.next();
                nextNeighbor = edge.getEndVertex();
            }
            else{
                throw new NoSuchElementException();
            }
            return nextNeighbor;
        }

        /**
         * Need to remove all edges of its neighbor
         */
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Warning: It's a thread-unsafe function
     * @param endVertex
     * @param edgeWeight
     * @return
     */
    public boolean connect(Vertex endVertex, int edgeWeight){
        boolean result = false;
        if(!this.equals(endVertex)) {
            Iterator<Vertex> it = this.getNeighborIterator();
            boolean duplicated = false; // No duplicated edge
            while(!duplicated && it.hasNext()) {
                Vertex next = it.next();
                if(endVertex.equals(next)) {
                    duplicated = true;
                }
            }
            if(!duplicated) {
                edgeList_.add(new Edge(this, endVertex, edgeWeight));
                endVertex.addIncomingVertex(vId_);
                this.increaseDegreeByOne(false);
                endVertex.increaseDegreeByOne(true);
                result = true;
            }
        }
        return result;
    }

    public boolean connect(Vertex endVertex) {
        return connect(endVertex, 0);
    }

    public Iterator<Vertex> getNeighborIterator() {
        return new NeighborIterator();
    }

    public Vertex getNeighborById(int tranId) {
        Vertex neighbor = null;
        Iterator<Vertex> it = this.getNeighborIterator();
        while(it.hasNext()) {
            Vertex next = it.next();
            if(next.getvId_() == tranId) {
                neighbor = next;
                break;
            }
        }
        return neighbor;
    }

    public Edge getEdgeById(int toId) {
        Edge edge = null;
        Iterator<Edge> it = edgeList_.iterator();
        while(it.hasNext()) {
            Edge next = it.next();
            if(next.getEndVertex().getvId_() == toId) {
                edge = next;
                break;
            }
        }
        return edge;
    }

    public Vertex getUnvisitedNeighbor() {
        Vertex unNeighbor = null;
        Iterator<Vertex> it = this.getNeighborIterator();
        while(it.hasNext() && unNeighbor == null) {
            Vertex next = it.next();
            if(!next.isVisited()) {
                unNeighbor = next;
            }
        }
        return unNeighbor;
    }

    public void removeNeighborById(int tranId) {
        if(getNeighborById(tranId) != null) {
            edgeList_.remove(new Edge(this, new Vertex(tranId), 0));
            --outDegree;
        }

    }

    public void addIncomingVertex(int incoming) {
        this.incomingEdge.add(incoming);
    }

    public void increaseDegreeByOne(boolean in) {
        if(in) {
            ++inDegree;
        }
        else {
            ++outDegree;
        }
    }

    public void decreaseDegreeByOne(boolean in) {
        if(in) {
            --inDegree;
        }
        else {
            --outDegree;
        }
    }

    public void visit() {
        visited_ = true;
    }

    public void unVisit() {
        visited_ = false;
    }

    public boolean isVisited() {
        return visited_;
    }

    public int getvId_() {
        return vId_;
    }

    public void setvId_(int vId_) {
        this.vId_ = vId_;
    }

    public List<Edge> getEdgeList_() {
        return edgeList_;
    }

    public void setEdgeList_(List<Edge> edgeList_) {
        this.edgeList_ = edgeList_;
    }

    public List<Integer> getIncomingEdge() {
        return incomingEdge;
    }

    public void setIncomingEdge(List<Integer> incomingEdge) {
        this.incomingEdge = incomingEdge;
    }

    public int getWeight_() {
        return weight_;
    }

    public void setWeight_(int weight_) {
        this.weight_ = weight_;
    }

    public int getLowLink() {
        return lowLink;
    }

    public void setLowLink(int lowLink) {
        this.lowLink = lowLink;
    }

    public int getDfNumber() {
        return dfNumber;
    }

    public void setDfNumber(int dfNumber) {
        this.dfNumber = dfNumber;
    }

    public int getInDegree() {
        return inDegree;
    }

    public void setInDegree(int inDegree) {
        this.inDegree = inDegree;
    }

    public int getOutDegree() {
        return outDegree;
    }

    public void setOutDegree(int outDegree) {
        this.outDegree = outDegree;
    }

    public void printVertex() {
        System.out.printf("Vertex_%d, Out: %d, In: %d, Weight: %d\n", vId_, outDegree, inDegree, weight_);
        Iterator<Vertex> it = this.getNeighborIterator();
        while(it.hasNext()) {
            System.out.print(it.next().getvId_() + " ");
        }
        System.out.print("\n");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Vertex vertex = (Vertex) o;
        return vId_ == vertex.vId_;
    }

    @Override
    public int hashCode() {
        return Objects.hash(vId_);
    }
}
