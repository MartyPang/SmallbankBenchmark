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

    /**
     * Zipfian Distribution
     */
//    private class ZipfGenerator {
//        private Random rnd = new Random(System.currentTimeMillis());
//        private int size;
//        private double skew;
//        private double bottom = 0;
//
//        public ZipfGenerator(int size, double skew) {
//            this.size = size;
//            this.skew = skew;
//
//            for(int i=1;i < size; i++) {
//                this.bottom += (1/Math.pow(i, this.skew));
//            }
//        }
//
//        // the next() method returns an random rank id.
//        // The frequency of returned rank ids are follows Zipf distribution.
//        public int next() {
//            int rank;
//            double friquency = 0;
//            double dice;
//
//            rank = rnd.nextInt(size);
//            friquency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
//            dice = rnd.nextDouble();
//
//            while(!(dice <= friquency)) {
//                rank = rnd.nextInt(size);
//                friquency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
//                dice = rnd.nextDouble();
//            }
//
//            return rank+1;
//        }
//
//        // This method returns a probability that the given rank occurs.
//        public double getProbability(int rank) {
//            return (1.0d / Math.pow(rank, this.skew)) / this.bottom;
//        }
//    }

    public class ZipfGenerator {
        private Random random = new Random(System.currentTimeMillis());
        private NavigableMap<Double, Integer> map;
        private static final double Constant = 1.0;

        public ZipfGenerator(int R, double F) {
            // create the TreeMap
            map = computeMap(R, F);
        }
        //size为rank个数，skew为数据倾斜程度, 取值为0表示数据无倾斜，取值越大倾斜程度越高
        private NavigableMap<Double, Integer> computeMap(
                int size, double skew) {
            NavigableMap<Double, Integer> map =
                    new TreeMap<Double, Integer>();
            //总频率
            double div = 0;
            //对每个rank，计算对应的词频，计算总词频
            for (int i = 1; i <= size; i++) {
                //the frequency in position i
                div += (Constant / Math.pow(i, skew));
            }
            //计算每个rank对应的y值，所以靠前rank的y值区间远比后面rank的y值区间大
            double sum = 0;
            for (int i = 1; i <= size; i++) {
                double p = (Constant / Math.pow(i, skew)) / div;
                sum += p;
                map.put(sum, i - 1);
            }
            return map;
        }

        public int next() {         // [1,n]
            double value = random.nextDouble();
            //找最近y值对应的rank
            return map.ceilingEntry(value).getValue() + 1;
        }

    }

    public void testZipf() {
        ZipfGenerator zipf = new ZipfGenerator(1000, 1);
        for(int i=0;i<100;++i) {
            System.out.println(zipf.next());
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
                    after.setParameters(before.getOp_(), before.getArgs_(), before.getRandoms());
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
            after.setParameters(before.getOp_(), before.getArgs_(), before.getRandoms());
            serialTasks.add(after);
        }
        return serialTasks;
    }
}
