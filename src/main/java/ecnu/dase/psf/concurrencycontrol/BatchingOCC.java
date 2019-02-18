package ecnu.dase.psf.concurrencycontrol;

import ecnu.dase.psf.common.Item;
import ecnu.dase.psf.common.Vertex;
import ecnu.dase.psf.smallbank.BatchSmallBankProcedure;

import java.util.*;

public class BatchingOCC {
    private int nValidation; // num of transactions request validation
    private int nCommit; // num of transactions committed
    /**
     * Transaction Dependency Graph
     */
    private DirectedGraph tdg;

    private TarjanSCC tarjan;

    public BatchingOCC() {
        nValidation = 0;
        nCommit = 0;
        tdg = new DirectedGraph();
        tarjan = new TarjanSCC();
    }

    public DirectedGraph constructConflictGraph(Map<Integer, BatchSmallBankProcedure> txs) {
        DirectedGraph cg = new DirectedGraph();
        // Add vertex for each transaction
        for(BatchSmallBankProcedure tx : txs.values()) {
            cg.addVertex(tx.getTranId_());
        }
        Map<Integer, Vertex> vertices = cg.getVertices();
        for(BatchSmallBankProcedure tx : txs.values()) {
            Map<String, Item> writeSet = tx.getWriteSet_();
            for(BatchSmallBankProcedure otherTx : txs.values()) {
                if(tx.equals(otherTx)) { //compare with other transaction's read set
                    continue;
                }
                if(hasConflict(writeSet, otherTx.getReadSet_())) {
                    /**
                     *         tx
                     *        /\
                     *        |
                     *       /
                     *      /
                     *     /
                     * otherTx
                     */
                    cg.addEdge(otherTx.getTranId_(), tx.getTranId_());
                }
            }
        }
        return cg;
    }

    /**
     * Check if t's read set has conflict with other t's write set
     * @param writeSet
     * @param readSet
     * @return true for having conflict
     *         false for not having any
     */
    private boolean hasConflict(Map<String, Item> writeSet, Map<String, Item> readSet) {
        if(null == writeSet || null == readSet) { // if one of them is null
            return  false;
        }
        boolean conflict = false;
        Set<String> readKeys = readSet.keySet();
        for(String key : readKeys) {
            if(writeSet.containsKey(key)) {
                conflict = true;
                break;
            }
        }
        return conflict;
    }

    public List<Integer> findAbortTransactionSet(DirectedGraph cg) {
        List<Integer> txSet = new ArrayList<>();
        //Get all SCCs using Tarjan's algorithm
        tarjan.runTarjan(cg);
        List<DirectedGraph> scc = tarjan.getScc();
        //tarjan.printSCC();
        //Need to copy scc and remove irrelevant edges
        List<DirectedGraph> copySCC = new ArrayList<>();
        for(DirectedGraph component : scc) {
            DirectedGraph copyComponent = new DirectedGraph();
            //Add vertices
            for(Integer vid : component.getVertexIdSet()) {
                copyComponent.addVertex(vid);
            }
            Iterator<Vertex> it = component.getVertices().values().iterator();
            while(it.hasNext()) {
                Vertex next = it.next();
                Iterator<Vertex> neighbors = next.getNeighborIterator();
                while (neighbors.hasNext()) {
                    //Only need to add relevant edges
                    Vertex relevant = neighbors.next();
                    if(component.getVertexIdSet().contains(relevant.getvId_())) {
                        copyComponent.addEdge(next.getvId_(), relevant.getvId_());
                    }
                }
            }
            copySCC.add(copyComponent);
        }
        //Run greedy select algorithm
        for(DirectedGraph component : copySCC) {
            //System.out.println("Original scc: ");
            //component.printGraph();
            txSet.addAll(greedySelectVertex(component));
        }
        //System.out.println("Abort: " + txSet);
        return txSet;
    }

    private List<Integer> greedySelectVertex(DirectedGraph component) {
        List<Integer> v = new ArrayList<>();
        if(component.getGraphSize() <= 1) {
            return v;
        }
        //Choose vertex to abort by strategy
        //Here we select one with minimal out-degree
        int min = Integer.MAX_VALUE;
        int minVid = 0;
        Iterator<Vertex> it = component.getVertices().values().iterator();
        while(it.hasNext()) {
            Vertex next = it.next();
            if(next.getOutDegree() < min) {
                min = next.getOutDegree();
                minVid = next.getvId_();
            }
        }
        //Remove abort vertex from component,
        //Trim vertex with no incoming or outgoing edge,
        //And run a greedySelectVertex with remaining graph
        component.removeVertex(minVid);
//        System.out.println("After remove: "+component.getGraphSize());
        v.add(minVid);
//        System.out.println("Before trim: ");
//        component.printGraph();
        component.trimGraph();
//        System.out.println("After trim: ");
//        component.printGraph();
        v.addAll(greedySelectVertex(component));
        return v;
    }

    public void commitTransaction(BatchSmallBankProcedure tx) {
        tx.Commit();
        ++nCommit;
        //add a new vertex to tdg
        tdg.addVertex(tx.getTranId_(), tx.getCost());
        //System.out.printf("Add vertex %d to tdg.\n", tx.getTranId_());
        //update edges
        Map<String, Item> readSet = tx.getReadSet_();
        Set<String> keyset = readSet.keySet();
        for(String key : keyset) {
            Item item = readSet.get(key);
            if(item.getWrittenBy_() != 0 && tdg.getVertexIdSet().contains(item.getWrittenBy_())) {
//                if(!tdg.hasEdge(item.getWrittenBy_(), tx.getTranId_())) {
//                }
                tdg.addEdge(item.getWrittenBy_(), tx.getTranId_());
                //update edge weight and consistent read set
                tdg.updateEdge(item.getWrittenBy_(), tx.getTranId_(), key, item);
            }
        }
    }

    public void reset() {
        nCommit = 0;
        tdg.reset();
    }

    public int getnCommit() {
        return nCommit;
    }

    public void setnCommit(int nCommit) {
        this.nCommit = nCommit;
    }

    public DirectedGraph getTdg() {
        return tdg;
    }

    public void setTdg(DirectedGraph tdg) {
        this.tdg = tdg;
    }
}
