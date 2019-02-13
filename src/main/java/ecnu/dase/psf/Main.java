package ecnu.dase.psf;

import ecnu.dase.psf.concurrencycontrol.Miner;
import ecnu.dase.psf.smallbank.SmallBankProcedure;
import ecnu.dase.psf.smallbank.WorkloadGenerator;
import ecnu.dase.psf.storage.DB;

import java.util.Map;

/**
 * @author MartyPang
 * @version 1.0
 * @date 2019/2/13 19:24
 */
public class Main {
    public static void main(String[] args) {
        Miner miner = new Miner();
        DB db = new DB(15, 10);
        WorkloadGenerator generator = new WorkloadGenerator(db, 10, 10, 10);
        Map<Integer, SmallBankProcedure> workload = generator.generateWorkload();
        miner.setBatch(workload);
        miner.concurrentMining();
        miner.shutdownPool();
    }
}
