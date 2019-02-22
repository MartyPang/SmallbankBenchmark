package ecnu.dase.psf;

import ecnu.dase.psf.concurrencycontrol.DirectedGraph;
import ecnu.dase.psf.concurrencycontrol.Miner;
import ecnu.dase.psf.concurrencycontrol.SerialRunner;
import ecnu.dase.psf.concurrencycontrol.Validator;
import ecnu.dase.psf.smallbank.*;
import ecnu.dase.psf.storage.DB;
import ecnu.dase.psf.storage.HybridDB;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author MartyPang
 * @version 1.0
 * @date 2019/2/13 19:24
 */
public class Main {
    public static void main(String[] args) {
        int threadNum;
        int accNum;
        int transactionNum;
        int k;
        double skew;
        if(0 == args.length) {
            threadNum = 4;
            accNum = 400;
            transactionNum = 400;
            k = threadNum;
            skew = 0.0; //uniform distribution
        }
        else if(1 == args.length) {
            threadNum = Integer.parseInt(args[0]);
            accNum = 400;
            transactionNum = 400;
            k = threadNum;
            skew = 0.0; //uniform distribution
        }
        else if(2 == args.length) {
            threadNum = Integer.parseInt(args[0]);
            accNum = Integer.parseInt(args[1]);
            transactionNum = 400;
            k = threadNum;
            skew = 0.0; //uniform distribution
        }
        else if(3 == args.length) {
            threadNum = Integer.parseInt(args[0]);
            accNum = Integer.parseInt(args[1]);
            transactionNum = Integer.parseInt(args[2]);
            k = threadNum;
            skew = 0.0; //uniform distribution
        }
        else if(4 == args.length) {
            threadNum = Integer.parseInt(args[0]);
            accNum = Integer.parseInt(args[1]);
            transactionNum = Integer.parseInt(args[2]);
            k = Integer.parseInt(args[3]);
            skew = 0.0; //uniform distribution
        }
        else if(5 == args.length) {
            threadNum = Integer.parseInt(args[0]);
            accNum = Integer.parseInt(args[1]);
            transactionNum = Integer.parseInt(args[2]);
            k = Integer.parseInt(args[3]);
            skew = Double.parseDouble(args[4]);
        }
        else {
            threadNum = 4;
            accNum = 400;
            transactionNum = 400;
            k = threadNum;
            skew = 0.0;
        }
        System.out.println(threadNum);
        ExecutorService pool = Executors.newFixedThreadPool(threadNum);
        Miner miner = new Miner(pool, 0.8, k);
        Validator validator = new Validator(pool);
        SerialRunner serial = new SerialRunner();
        int timeM = 0;
        int timeV = 0;
        int timeV2 = 0;
        int timeS = 0;
        for(int i = 0; i < 5; ++i) {
            miner.reset();
            DB dbMiner = new DB(100000, 10);
            WorkloadGenerator generator = new WorkloadGenerator(dbMiner, transactionNum, accNum, 10, skew);
            Map<Integer, BatchSmallBankProcedure> workloadM= generator.generateBatchWorkload();
            miner.setBatch(workloadM);
            Long startM = System.currentTimeMillis();
            DirectedGraph tdg = miner.concurrentMining();
            List<DirectedGraph> partition = miner.kWayPartition(tdg);
            Long endM = System.currentTimeMillis();
            //System.out.println((endM - startM) + " " + (endM2 - endM));
            if(i != 0) {
                timeM += (endM - startM);
            }
//            System.out.println("Miner execution time: " + (endM -startM));
//            System.out.println("---------------------------------------------");

            validator.reset();
            DB dbVal = new DB(100000, 10);
            HybridDB hdb = new HybridDB(dbVal, tdg);
            List<DeProcedure> workloadV = generator.transformDeWorkload(miner.getBatch(), hdb, partition, tdg.getTopologicalSort());
            validator.setTasks(workloadV);
            Map<Integer, DeSmallBank> allTx = new HashMap<>();
            for(DeProcedure subtask : workloadV) {
                List<DeSmallBank> partTask = subtask.getTasks();
                for(DeSmallBank each : partTask) {
                    allTx.put(each.getTranId_(), each);
                }
            }
            validator.setAllTasks(allTx);
            validator.setTopologic(tdg.getTopologicalSort());
            validator.setCommitProcedure();
            Long startV = System.currentTimeMillis();
            validator.concurrentValidate();
            Long endV = System.currentTimeMillis();
            if(i != 0) {
                timeV += (endV - startV);
            }
//            System.out.println("Validator execution time: " + (endV - startV));
//            System.out.println("---------------------------------------------");

            //without partition
            Long startV2 = System.currentTimeMillis();
            List<Future<Long>> futureList;
            try {
                futureList = pool.invokeAll(allTx.values(), 5, TimeUnit.MINUTES);
                for(Future<Long> f : futureList) {
                    f.get();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            Long endV2 = System.currentTimeMillis();
            if(i != 0) {
                timeV2 += (endV2 - startV2);
            }

            DB dbSerial = new DB(100000, 10);
            List<SerialProcedure> workloadS = generator.trandformSerialWorkload(miner.getBatch(), dbSerial, tdg.getTopologicalSort());
            serial.setSerialTasks(workloadS);
            Long startS = System.currentTimeMillis();
            serial.serialExecute();
            Long endS = System.currentTimeMillis();
            if(i != 0) {
                timeS += (endS - startS);
            }
//            System.out.println("Serial execution time: " + (endS - startS));
        }
        miner.shutdownPool();
        validator.shutdownPool();
        serial.shutdownPool();
        System.out.println("Miner: " + timeM/4);
        System.out.println("Validator: " + timeV/4);
        System.out.println("Validator V2: " + timeV2/4);
        System.out.println("Serial: " + timeS/4);
    }
}
