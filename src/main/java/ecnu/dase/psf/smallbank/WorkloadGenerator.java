package ecnu.dase.psf.smallbank;

import ecnu.dase.psf.concurrencycontrol.DirectedGraph;
import ecnu.dase.psf.storage.DB;
import ecnu.dase.psf.storage.HybridDB;

import java.util.*;

/**
 * Generate small bank benchmark workload
 * @author MartyPang
 * @version 1.0
 * @date 2019/2/11 20:11
 */
public class WorkloadGenerator {
    DB db;
    private int numTx;
    private int numAcc;
    private int bal;

    public WorkloadGenerator(DB database, int batchSize, int accountNumber, int balance) {
        db = database;
        numTx = batchSize;
        numAcc = accountNumber;
        bal = balance;
    }

    private class UniformGenerator {
        Random rand;
        int max, min;
        public UniformGenerator(int Min, int Max) {
            rand = new Random();
            min = Min;
            max = Max;
        }

        public int nextInt() {
            return rand.nextInt(max-min+1)+min;
        }
    }

    public Map<Integer, BatchSmallBankProcedure> generateBatchWorkload() {
        Map<Integer, BatchSmallBankProcedure> workload = new HashMap<>();
        UniformGenerator tx_gen = new UniformGenerator(1, 5);
        UniformGenerator acc_gen = new UniformGenerator(1, numAcc);
        UniformGenerator bal_gen = new UniformGenerator(1, bal);

        for(int i= 1;i <= numTx; ++i) {
            int tx = tx_gen.nextInt();
            int[] args;
            switch (tx) {
                case 1:
                    args = new int[2];
                    args[0] = acc_gen.nextInt();
                    args[1] = acc_gen.nextInt();
                    break;
                case 2:
                case 3:
                case 4:
                    args = new int[2];
                    args[0] = acc_gen.nextInt();
                    args[1] = bal_gen.nextInt();
                    break;
                case 5:
                    args = new int[3];
                    args[0] = acc_gen.nextInt();
                    args[1] = acc_gen.nextInt();
                    args[2] = bal_gen.nextInt();
                    break;
                default:
                    args = new int[2];
                    break;
            }
            BatchSmallBankProcedure transaction = new BatchSmallBankProcedure(db, i);
            transaction.setParameters(tx, args);
            workload.put(i, transaction);
        }
        return workload;
    }

    public List<DeProcedure> transformDeWorkload(Map<Integer, BatchSmallBankProcedure> batchWorkload, HybridDB hdb, List<DirectedGraph> partition, Stack<Integer> topologic) {
        List<DeProcedure> tasks = new ArrayList<>();
        List<List<DeSmallBank>> deWorkload = new ArrayList<>();
        List<Integer> orders = new ArrayList<>();
        //initialize deWorkload
        for(int i = 0; i < partition.size(); ++i) {
            List<DeSmallBank> piece = new ArrayList<>();
            deWorkload.add(piece);
        }
        while(!topologic.empty()) {
            int tranId = topologic.pop();
            for(int i = 0; i < partition.size(); ++i) {
                DirectedGraph subgraph = partition.get(i);
                if(subgraph.getVertexIdSet().contains(tranId)) {
                    BatchSmallBankProcedure before = batchWorkload.get(tranId);
                    DeSmallBank after = new DeSmallBank(hdb, tranId);
                    after.setParameters(before.getOp_(), before.getArgs_());
                    deWorkload.get(i).add(after);
                }
            }
        }
        for(List<DeSmallBank> subtask : deWorkload) {
            //System.out.println("Subtask size: " + subtask.size());
            DeProcedure p = new DeProcedure(subtask);
            tasks.add(p);
        }
        return tasks;
    }

    public List<SerialProcedure> trandformSerialWorkload(Map<Integer, BatchSmallBankProcedure> batch, DB sdb, Stack<Integer> topologic) {
        List<SerialProcedure> serialTasks = new ArrayList<>();
        while(!topologic.empty()) {
            int tranId = topologic.pop();
            BatchSmallBankProcedure before = batch.get(tranId);
            SerialProcedure after = new SerialProcedure(sdb, tranId);
            after.setParameters(before.getOp_(), before.getArgs_());
            serialTasks.add(after);
        }
        return serialTasks;
    }
}
