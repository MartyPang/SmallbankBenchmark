package ecnu.dase.psf;

import ecnu.dase.psf.common.Edge;
import ecnu.dase.psf.concurrencycontrol.DirectedGraph;
import ecnu.dase.psf.concurrencycontrol.Miner;
import ecnu.dase.psf.smallbank.BatchSmallBankProcedure;
import ecnu.dase.psf.smallbank.WorkloadGenerator;
import ecnu.dase.psf.storage.DB;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author MartyPang
 * @version 1.0
 * @date 2019/2/20 15:32
 */
public class kMain {
    public static void main(String[] args) {
        int threadNum = 16;
        int accNum = 100;
        int transactionNum = 400;
        int k;
        if(0 == args.length) {
            k = 4;
        }
        else {
            k = Integer.parseInt(args[0]);
        }
        ExecutorService pool = Executors.newFixedThreadPool(threadNum);
        Miner miner = new Miner(pool, 0.8, k);
        int totalWeight = 0;
        int edgeCut = 0;
        for(int i = 0; i < 10; ++i) {
            miner.reset();
            DB db = new DB(100000, 10);
            WorkloadGenerator generator = new WorkloadGenerator(db, transactionNum, accNum, 10);
            Map<Integer, BatchSmallBankProcedure> workload = generator.generateBatchWorkload();
            miner.setBatch(workload);
            DirectedGraph tdg = miner.concurrentMining();
            //tdg.printGraph();
            List<DirectedGraph> partition = miner.kWayPartition(tdg);
            totalWeight += tdg.getGraphEdgeWeight();
            //System.out.println("total: " + totalWeight);
            List<Edge> edges = tdg.getSortedEdges();
            for(Edge e : edges) {
                int u = e.getStartVertex().getvId_();
                int v = e.getEndVertex().getvId_();
                boolean isCut = true;
                for(DirectedGraph subgraph : partition) {
                    if(subgraph.getVertexIdSet().contains(u) && subgraph.getVertexIdSet().contains(v)) {
                        isCut = false;
                        break;
                    }
                }
                if(isCut) {
                    edgeCut += e.getWeight();
                    //System.out.println("cut: " + edgeCut);
                }
            }
        }
        miner.shutdownPool();
        System.out.printf("k: %d %.3f\n", k, (1.0*edgeCut)/totalWeight);
        System.out.println("total: " + totalWeight);
        System.out.println("cut: " + edgeCut);
    }

}
