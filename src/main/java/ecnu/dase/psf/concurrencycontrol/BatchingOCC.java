package ecnu.dase.psf.concurrencycontrol;

import ecnu.dase.psf.common.Item;
import ecnu.dase.psf.common.Vertex;
import ecnu.dase.psf.smallbank.SmallBankConstants;
import ecnu.dase.psf.smallbank.SmallBankProcedure;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class BatchingOCC {
    private int nValidation; // num of transactions request validation
    private int nCommit; // num of transactions committed

    private ExecutorService pool;
    private TarjanSCC tarjan;

    public BatchingOCC() {
        nValidation = 0;
        nCommit = 0;
        pool = Executors.newFixedThreadPool(SmallBankConstants.DEFAULT_THREAD);
        tarjan = new TarjanSCC();
    }

    public BatchingOCC(int thread_num) {
        nValidation = 0;
        nCommit = 0;
        pool = Executors.newFixedThreadPool(thread_num);
        tarjan = new TarjanSCC();
    }

    public void executeParallel(List<SmallBankProcedure> txs) {
        List<Future<Long>> futureList = new ArrayList<>();

        try{
            futureList = pool.invokeAll(txs, 1, TimeUnit.MINUTES);
            for(Future<Long> f : futureList) {
                Long execution_time = f.get();
                //TODO - Set up the weigh of transactions
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public ConflictGraph constructCG(List<SmallBankProcedure> txs) {
        ConflictGraph cg = new ConflictGraph();
        // Add vertex for each transaction
        for(SmallBankProcedure tx : txs) {
            cg.addVertex(tx.getTranId_());
        }
        Map<Integer, Vertex> vertices = cg.getVertices();
        for(SmallBankProcedure tx : txs) {
            Map<String, Item> writeSet = tx.getWriteSet_();
            for(SmallBankProcedure otherTx : txs) {
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

    public List<Integer> findAbortTransactionSet(ConflictGraph cg) {
        List<Integer> txSet = new ArrayList<>();
        //Get all SCCs using Tarjan's algorithm
        tarjan.runTarjan(cg);
        List<Map<Integer, Vertex>> scc = tarjan.getScc();
        //Run greedy select algorithm
        for(Map<Integer, Vertex> component : scc) {
            txSet.addAll(greedySelectVertex(component));
        }
        return txSet;
    }

    private List<Integer> greedySelectVertex(Map<Integer, Vertex> component) {
        List<Integer> v = new ArrayList<>();
        if(component.size() <= 1) {
            return v;
        }
        //Choose vertex to abort by strategy

        //Remove abort vertex from component,
        //Trim vertex with no incoming or outgoing edge,
        //And run a greedySelectVertex with remaining graph

        return v;
    }

}
