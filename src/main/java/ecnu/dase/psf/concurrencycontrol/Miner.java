package ecnu.dase.psf.concurrencycontrol;

import ecnu.dase.psf.common.Edge;
import ecnu.dase.psf.common.Vertex;
import ecnu.dase.psf.smallbank.BatchSmallBankProcedure;
import ecnu.dase.psf.smallbank.SmallBankConstants;

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
    private int k; //k-way partition
    private Map<Integer, BatchSmallBankProcedure> batch;

    public Miner() {
        bocc = new BatchingOCC();
        pool = Executors.newFixedThreadPool(SmallBankConstants.DEFAULT_THREAD);
        commitRatio = 0.8;
        k = 1;
        batch = new HashMap<>();
    }

    public Miner(int thread_num, double cr, int k) {
        bocc = new BatchingOCC();
        pool = Executors.newFixedThreadPool(thread_num);
        commitRatio = cr;
        this.k = k;
        batch = new HashMap<>();
    }

    /**
     * Execute batch transactions concurrently
     * @return Transaction Dependency Graph
     */
    public DirectedGraph concurrentMining() {
        Map<Integer, BatchSmallBankProcedure> txs = new HashMap<>(batch);
        //txs.putAll(batch);
        while (bocc.getnCommit() < (int)(commitRatio*batch.size())) {
            //Execute transactions in parallel
            executeParallel(txs);
            DirectedGraph cg = bocc.constructConflictGraph(txs);
            //cg.printGraph();
            List<Integer> abortSet = bocc.findAbortTransactionSet(cg);
            //System.out.println("abortSet: "+abortSet);
            //remove abort vertex from cg
            for(int vid : abortSet) {
                cg.removeVertex(vid);
            }
            //Get topological order,
            //and commit tx according to the pop order
            Stack<Integer> topology = cg.getTopologicalSort();
            //System.out.println(topology);
            while(!topology.empty()) {
                int commitId = topology.pop();
                BatchSmallBankProcedure commitTx = txs.get(commitId);
//                System.out.println(commitTx.getReadSet_());
//                System.out.println(commitTx.getWriteSet_());
                bocc.commitTransaction(commitTx);
                //remove from batch
                txs.remove(commitId);
            }
            //Reset abort transactions,
            //get ready for re-execution
            Iterator<BatchSmallBankProcedure> it = txs.values().iterator();
            while(it.hasNext()) {
                it.next().Reset();
            }
            //System.out.printf("commit: %d threshold: %d\n", bocc.getnCommit(), (int)(commitRatio*batch.size()));
            //bocc.getTdg().printGraph();
            //System.out.println("------------------------END-------------------------");
        }
        //remove abort transactions from batch
        for(int abort : txs.keySet()) {
            batch.remove(abort);
        }
        return bocc.getTdg();
    }

    public void executeParallel(Map<Integer, BatchSmallBankProcedure> txs) {
        List<Future<Long>> futureList;

        try{
            futureList = pool.invokeAll(txs.values(), 1, TimeUnit.MINUTES);
            Iterator<BatchSmallBankProcedure> it = txs.values().iterator();
            for(Future<Long> f : futureList) {
                Long execution_time = f.get();
                //System.out.println("execution time: "+execution_time);
                //Set up the weigh of transactions
                if(it.hasNext()) {
                    BatchSmallBankProcedure next = it.next();
                    next.setCost(Integer.parseInt(execution_time.toString()));
                    //System.out.println("cost: "+next.getCost());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<DirectedGraph> kWayPartition(DirectedGraph tdg) {
        List<DirectedGraph> partition = new ArrayList<>();
        //Calculate weight
        int totalCost = tdg.getGraphWeight();
        int upperBound = totalCost/k;
        //System.out.println("upper bound: "+upperBound);
        int index = 0;
        int cost = 0;
        for(int i=0;i<k;++i) {
            DirectedGraph part = new DirectedGraph();
            partition.add(part);
        }
        List<Edge> edges = tdg.getSortedEdges();
        //System.out.println("Edge list: " + edges.size());
        for(Edge e : edges){
            Vertex u = e.getStartVertex();
            Vertex v = e.getEndVertex();
            if(!u.isVisited() && !v.isVisited()) {
                int tmp = cost;
                tmp += u.getWeight_();
                tmp += v.getWeight_();
                if(tmp >= upperBound) {
                    cost = 0;
                    if(index + 1 < k) {
                        ++index;
                    }
                }
                u.visit();
                v.visit();
                partition.get(index).addVertex(u);
                partition.get(index).addVertex(v);
                cost += u.getWeight_();
                cost += v.getWeight_();
            }
            else if(!u.isVisited() && v.isVisited()) {
                int tmp = cost;
                tmp += u.getWeight_();
                if(tmp >= upperBound) {
                    cost = 0;
                    if(index + 1 < k) {
                        ++index;
                    }
                }
                u.visit();
                partition.get(index).addVertex(u);
                cost += u.getWeight_();
            }
            else if(u.isVisited() && !v.isVisited()) {
                int tmp = cost;
                tmp += v.getWeight_();
                if(tmp >= upperBound) {
                    cost = 0;
                    if(index + 1 < k) {
                        ++index;
                    }
                }
                v.visit();
                partition.get(index).addVertex(v);
                cost += v.getWeight_();
            }
        }
        for(int vId : tdg.getVertexIdSet()) {
            Vertex vertex = tdg.getVertices().get(vId);
            if(!vertex.isVisited()) {
                int tmp = cost;
                tmp += vertex.getWeight_();
                if(tmp >= upperBound) {
                    cost = 0;
                    if(index + 1 < k) {
                        ++index;
                    }
                }
                vertex.visit();
                partition.get(index).addVertex(vertex);
                cost += vertex.getWeight_();
                //System.out.printf("Add vertex %d to weight %d partition %d\n", vertex.getvId_(), vertex.getWeight_(), index);
            }
        }
        return partition;
    }

    public void reset() {
        batch.clear();
        bocc.reset();
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

    public int getK() {
        return k;
    }

    public void setK(int k) {
        this.k = k;
    }

    public Map<Integer, BatchSmallBankProcedure> getBatch() {
        return batch;
    }

    public void setBatch(Map<Integer, BatchSmallBankProcedure> batch) {
        this.batch = batch;
    }
}
