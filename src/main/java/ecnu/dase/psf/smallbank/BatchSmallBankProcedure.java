package ecnu.dase.psf.smallbank;

import ecnu.dase.psf.common.Item;
import ecnu.dase.psf.storage.DB;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author MartyPang
 * @version 1.0
 * @date 2019/2/13 16:22
 */
public class BatchSmallBankProcedure implements Callable<Long> {
    /**
     * Batching OCC protocol
     */
    //BatchingOCC bOCC_;
    /**
     * simulated state database
     */
    private DB db_;

    /**
     * Transaction info
     */
    private int tranId_;
    /**
     * Key: TableName_Index
     * Value: read value
     */
    private Map<String, Item> readSet_;
    /**
     * Key: TableName_Index
     * Value: write value
     */
    private Map<String, Item> writeSet_;

    /**
     * Execution time
     */
    private int cost;

    /**
     * operation code
     * 1 --- Amalgamate
     * 2 --- WriteCheck
     * 3 --- DepositChecking
     * 4 --- TransactSaving
     * 5 --- SendPayment
     * 6 --- Commit
     */
    private int op_;
    private int[] args_;
    private int[] randoms;

    ThreadLocalRandom localR = ThreadLocalRandom.current();


    public BatchSmallBankProcedure(DB db, int tranId) {
        //global variables
        db_ = db;
        tranId_ = tranId;
        readSet_ = new HashMap<>();
        writeSet_ = new HashMap<>();
        randoms = new int[10];
        cost = 0;
    }

    public void setParameters(int op, int[] args) {
        op_ = op;
        args_ = args;
    }

    /**
     *
     * @return the execution time
     * @throws Exception
     */
    @Override
    public Long call() throws Exception {
        Long start = System.currentTimeMillis();
        switch(op_) {
            case 1:
                for(int i = 0; i < 6; ++i) {
                    randoms[i] = localR.nextInt()%1500 + 1500;
                }
                Amalgamate(args_[0], args_[1]);
                break;
            case 2:
                for(int i = 0; i < 4; ++i) {
                    randoms[i] = localR.nextInt()%1500 + 1500;
                }
                WriteCheck(args_[0], args_[1]);
                break;
            case 3:
                for(int i = 0; i < 3; ++i) {
                    randoms[i] = localR.nextInt()%1500 + 1500;
                }
                DepositChecking(args_[0], args_[1]);
                break;
            case 4:
                for(int i = 0; i < 3; ++i) {
                    randoms[i] = localR.nextInt()%1500 + 1500;
                }
                TransactSaving(args_[0], args_[1]);
                break;
            case 5:
                for(int i = 0; i < 6; ++i) {
                    randoms[i] = localR.nextInt()%1500 + 1500;
                }
                SendPayment(args_[0], args_[1], args_[2]);
                break;
            case 6:
                Commit();
                break;
            default:
        }
        Long end = System.currentTimeMillis();
        return end - start;
    }

    /**
     * The Amalgamate transfers the entire contents of one
     * customer's savings account into another customer's checking
     * account.
     * @param acc1
     * @param acc2
     */
    private void Amalgamate(int acc1, int acc2) {
        // get accounts
        Item acc_1 = db_.getState(SmallBankConstants.ACCOUNTS_TAB, acc1, randoms[0]);
        Item acc_2 = db_.getState(SmallBankConstants.ACCOUNTS_TAB, acc2, randoms[1]);
        // add to read set
        readSet_.put(SmallBankConstants.ACCOUNTS_TAB+"_"+acc1, acc_1);
        readSet_.put(SmallBankConstants.ACCOUNTS_TAB+"_"+acc2, acc_2);

        Item bal1 = db_.getState(SmallBankConstants.SAVINGS_TAB, acc1, randoms[2]);
        Item bal2 = db_.getState(SmallBankConstants.CHECKINGS_TAB, acc2, randoms[3]);
        int total = bal1.getValue_()+bal2.getValue_();
        // add to read set
        readSet_.put(SmallBankConstants.SAVINGS_TAB+"_"+acc1, bal1);
        readSet_.put(SmallBankConstants.CHECKINGS_TAB+"_"+acc2, bal2);

        // add to write set
        deferredWrite(SmallBankConstants.SAVINGS_TAB+"_"+acc1, new Item(0, tranId_), randoms[4]);
        deferredWrite(SmallBankConstants.CHECKINGS_TAB+"_"+acc2, new Item(total, tranId_), randoms[5]);
    }

    /**
     * The WriteCheck removes an amount from the customer's
     * checking account.
     * @param acc
     * @param amount
     */
    private void WriteCheck(int acc, int amount) {
        // get account
        Item acc_ = db_.getState(SmallBankConstants.ACCOUNTS_TAB, acc, randoms[0]);
        //add to read set
        readSet_.put(SmallBankConstants.ACCOUNTS_TAB+"_"+acc, acc_);

        Item bal1 = db_.getState(SmallBankConstants.SAVINGS_TAB, acc, randoms[1]);
        Item bal2 = db_.getState(SmallBankConstants.CHECKINGS_TAB, acc, randoms[2]);
        int total = bal1.getValue_() + bal2.getValue_();
        // add to read set
        readSet_.put(SmallBankConstants.SAVINGS_TAB+"_"+acc, bal1);
        readSet_.put(SmallBankConstants.CHECKINGS_TAB+"_"+acc, bal2);

        // write check, add penality if overdraft
        if(total < amount) {
            deferredWrite(SmallBankConstants.CHECKINGS_TAB+"_"+acc, new Item(amount - 1, tranId_), randoms[3]);
        }else {
            deferredWrite(SmallBankConstants.CHECKINGS_TAB+"_"+acc, new Item(amount, tranId_), randoms[3]);
        }
    }

    private void DepositChecking(int acc, int amount) {
        // get account
        Item acc_ = db_.getState(SmallBankConstants.ACCOUNTS_TAB, acc, randoms[0]);
        //add to read set
        readSet_.put(SmallBankConstants.ACCOUNTS_TAB+"_"+acc, acc_);

        Item bal = db_.getState(SmallBankConstants.CHECKINGS_TAB, acc, randoms[1]);
        readSet_.put(SmallBankConstants.CHECKINGS_TAB+"_"+acc, bal);

        deferredWrite(SmallBankConstants.CHECKINGS_TAB+"_"+acc, new Item(bal.getValue_()+amount, tranId_), randoms[2]);
    }

    private void TransactSaving(int acc, int amount) {
        // get account
        Item acc_ = db_.getState(SmallBankConstants.ACCOUNTS_TAB, acc, randoms[0]);
        //add to read set
        readSet_.put(SmallBankConstants.ACCOUNTS_TAB+"_"+acc, acc_);

        Item bal = db_.getState(SmallBankConstants.SAVINGS_TAB, acc, randoms[1]);
        readSet_.put(SmallBankConstants.SAVINGS_TAB+"_"+acc, bal);

        deferredWrite(SmallBankConstants.CHECKINGS_TAB+"_"+acc, new Item(bal.getValue_()+amount, tranId_), randoms[2]);
    }

    private void SendPayment(int acc1, int acc2, int amount) {
        // get accounts
        Item acc_1 = db_.getState(SmallBankConstants.ACCOUNTS_TAB, acc1, randoms[0]);
        Item acc_2 = db_.getState(SmallBankConstants.ACCOUNTS_TAB, acc2, randoms[1]);
        // add to read set
        readSet_.put(SmallBankConstants.ACCOUNTS_TAB+"_"+acc1, acc_1);
        readSet_.put(SmallBankConstants.ACCOUNTS_TAB+"_"+acc2, acc_2);

        // get checking balance
        Item bal1 = db_.getState(SmallBankConstants.CHECKINGS_TAB, acc1, randoms[2]);
        Item bal2 = db_.getState(SmallBankConstants.CHECKINGS_TAB, acc2, randoms[3]);
        // add to read set
        readSet_.put(SmallBankConstants.SAVINGS_TAB+"_"+acc1, bal1);
        readSet_.put(SmallBankConstants.CHECKINGS_TAB+"_"+acc2, bal2);

        // add to write set
        deferredWrite(SmallBankConstants.SAVINGS_TAB+"_"+acc1, new Item(bal1.getValue_()-amount, tranId_), randoms[4]);
        deferredWrite(SmallBankConstants.CHECKINGS_TAB+"_"+acc2, new Item(bal2.getValue_()+amount, tranId_), randoms[5]);
    }

    public void deferredWrite(String key, Item item, int internal) {
        for(int i = 0;i<20;++i){
            for(int j = 0;j<internal;++j){
                isPrime(i*j);
            }
        }
        writeSet_.put(key, item);
    }

    public void Commit() {
        String table;
        int acc = 0;
        for(String key : writeSet_.keySet()) {
            String[] args = key.split("_");
            table = args[0];
            acc = Integer.parseInt(args[1]);
            db_.putState(tranId_, table, acc, writeSet_.get(key).getValue_());
        }
    }

    public void Reset() {
        readSet_.clear();
        writeSet_.clear();
    }

    public boolean isPrime(int a) {
        boolean flag = true;
        if (a < 2) {// 素数不小于2
            return false;
        } else {
            for (int i = 2; i <= Math.sqrt(a); i++) {
                if (a % i == 0) {// 若能被整除，则说明不是素数，返回false
                    flag = false;
                    // break;// 跳出循环
                }
            }
        }
        return flag;
    }

    public int getTranId_() {
        return tranId_;
    }

    public void setTranId_(int tranId_) {
        this.tranId_ = tranId_;
    }

    public Map<String, Item> getReadSet_() {
        return readSet_;
    }

    public void setReadSet_(Map<String, Item> readSet_) {
        this.readSet_ = readSet_;
    }

    public Map<String, Item> getWriteSet_() {
        return writeSet_;
    }

    public void setWriteSet_(Map<String, Item> writeSet_) {
        this.writeSet_ = writeSet_;
    }

    public int getCost() {
        return cost;
    }

    public void setCost(int cost) {
        this.cost = cost;
    }

    public int getOp_() {
        return op_;
    }

    public void setOp_(int op_) {
        this.op_ = op_;
    }

    public int[] getArgs_() {
        return args_;
    }

    public void setArgs_(int[] args_) {
        this.args_ = args_;
    }

    public int[] getRandoms() {
        return randoms;
    }

    public void setRandoms(int[] randoms) {
        this.randoms = randoms;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BatchSmallBankProcedure that = (BatchSmallBankProcedure) o;
        return tranId_ == that.tranId_;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tranId_);
    }
}
