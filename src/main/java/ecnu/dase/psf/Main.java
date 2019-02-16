package ecnu.dase.psf;

import ecnu.dase.psf.concurrencycontrol.DirectedGraph;
import ecnu.dase.psf.concurrencycontrol.Miner;
import ecnu.dase.psf.smallbank.BatchSmallBankProcedure;
import ecnu.dase.psf.smallbank.WorkloadGenerator;
import ecnu.dase.psf.storage.DB;

import java.util.List;
import java.util.Map;

/**
 * @author MartyPang
 * @version 1.0
 * @date 2019/2/13 19:24
 */
public class Main {
    public static void main(String[] args) {
        Miner miner = new Miner(4, 0.8, 6);
        DB db = new DB(100000, 10);
        WorkloadGenerator generator = new WorkloadGenerator(db, 400, 100, 10);
        Map<Integer, BatchSmallBankProcedure> workload = generator.generateBatchWorkload();
        miner.setBatch(workload);
        DirectedGraph tdg = miner.concurrentMining();
        //tdg.printGraph();
        List<DirectedGraph> partition = miner.kWayPartition(tdg);
        for(DirectedGraph part : partition) {
            System.out.println(part.getGraphWeight());
        }
        miner.shutdownPool();
    }
}
