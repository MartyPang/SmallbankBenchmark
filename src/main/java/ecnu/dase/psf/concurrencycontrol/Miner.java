package ecnu.dase.psf.concurrencycontrol;

import ecnu.dase.psf.smallbank.SmallBankConstants;
import ecnu.dase.psf.smallbank.SmallBankProcedure;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author MartyPang
 * @version 1.0
 * @date 2019/2/12 13:57
 */
public class Miner {
    private BatchingOCC bocc;

    private ExecutorService pool;
    private double commitRatio;
    private Map<Integer, SmallBankProcedure> batch;

    public Miner() {
        bocc = new BatchingOCC();
        pool = Executors.newFixedThreadPool(SmallBankConstants.DEFAULT_THREAD);
        commitRatio = 0.8;
        batch = new HashMap<>();
    }

    public Miner(int thread_num, double cr) {
        bocc = new BatchingOCC();
        pool = Executors.newFixedThreadPool(thread_num);
        commitRatio = cr;
        batch = new HashMap<>();
    }

    /**
     * Execute batch transactions concurrently
     * @return Transaction Dependency Graph
     */
    public DirectedGraph concurrentMining() {
        Map<Integer, SmallBankProcedure> txs = new HashMap<>(batch);
        //txs.putAll(batch);
        while (bocc.getnCommit() < (int)(commitRatio*batch.size())) {
            //Execute transactions in parallel
            executeParallel(txs);
            DirectedGraph cg = bocc.constructConflictGraph(txs);
            cg.printGraph();
            List<Integer> abortSet = bocc.findAbortTransactionSet(cg);
            System.out.println("abortSet: "+abortSet);
            //remove abort vertex from cg
            for(int vid : abortSet) {
                cg.removeVertex(vid);
            }
            //Get topological order,
            //and commit tx according to the pop order
            Stack<Integer> topology = cg.getTopologicalSort();
            System.out.println(topology);
            while(!topology.empty()) {
                int commitId = topology.pop();
                SmallBankProcedure commitTx = txs.get(commitId);
                System.out.println(commitTx.getReadSet_());
                System.out.println(commitTx.getWriteSet_());
                bocc.commitTransaction(commitTx);
                //remove from batch
                txs.remove(commitId);
            }
            //Reset abort transactions,
            //get ready for re-execution
            Iterator<SmallBankProcedure> it = txs.values().iterator();
            while(it.hasNext()) {
                it.next().Reset();
            }
            System.out.printf("commit: %d\nthreshold: %d\n", bocc.getnCommit(), (int)(commitRatio*batch.size()));
        }
        return bocc.getTdg();
    }

    public void executeParallel(Map<Integer, SmallBankProcedure> txs) {
        List<Future<Long>> futureList;

        try{
            futureList = pool.invokeAll(txs.values(), 1, TimeUnit.MINUTES);
            for(Future<Long> f : futureList) {
                Long execution_time = f.get();
                //TODO - Set up the weigh of transactions
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void shutdownPool() {
        pool.shutdown();
    }

    public double getCommitRatio() {
        return commitRatio;
    }

    public void setCommitRatio(double commitRatio) {
        this.commitRatio = commitRatio;
    }

    public Map<Integer, SmallBankProcedure> getBatch() {
        return batch;
    }

    public void setBatch(Map<Integer, SmallBankProcedure> batch) {
        this.batch = batch;
    }
}
