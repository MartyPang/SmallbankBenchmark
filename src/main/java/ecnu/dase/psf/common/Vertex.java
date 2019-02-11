package ecnu.dase.psf.common;

import java.util.*;

public class Vertex {
    private int vId_; // transaction id
    private List<Edge> edgeList_; // outgoing edge list
    private boolean visited_;
    private double weight_; // execution time
    private int inDegree;
    private int outDegree;
    private int lowLink;


    public Vertex(int vId_) {
        this.vId_ = vId_;
        edgeList_ = new LinkedList<>();
        visited_ = false;
        weight_ = 0.0;
        lowLink = 0;
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
    public boolean connect(Vertex endVertex, double edgeWeight){
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
        }

    }

    public void increaseDegreeByOne(boolean in) {
        if(in) {
            ++inDegree;
        }
        else {
            ++outDegree;
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

    public double getWeight_() {
        return weight_;
    }

    public void setWeight_(double weight_) {
        this.weight_ = weight_;
    }

    public int getLowLink() {
        return lowLink;
    }

    public void setLowLink(int lowLink) {
        this.lowLink = lowLink;
    }

    public void printVertex() {
        System.out.printf("Vertex %d: ", vId_);
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
