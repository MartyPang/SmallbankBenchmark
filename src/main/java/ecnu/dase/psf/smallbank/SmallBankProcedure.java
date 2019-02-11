package ecnu.dase.psf.smallbank;

import ecnu.dase.psf.common.Item;
import ecnu.dase.psf.concurrencycontrol.BatchingOCC;
import ecnu.dase.psf.storage.DB;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;

public class SmallBankProcedure implements Callable<Long> {
    /**
     * Batching OCC protocol
     */
    BatchingOCC bOCC_;
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
     * operation code
     * 1 --- Amalgamate
     * 2 --- WriteCheck
     * 3 --- DepositChecking
     * 4 --- TransactSaving
     * 5 --- SendPayment
     * 6 --- Commit
     */
    int op_;
    int[] args_;



    public SmallBankProcedure(BatchingOCC bOCC, DB db, int tranId) {
        //global variables
        bOCC_ = bOCC;
        db_ = db;
        tranId_ = tranId;
        readSet_ = new HashMap<>();
        writeSet_ = new HashMap<>();
    }

    public void setParameters(int op, int[] args) {
        op_ = op;
        args_ = args;
    }

    @Override
    public Long call() throws Exception {
        switch(op_) {
            case 1:
                Amalgamate(args_[0], args_[1]);
                break;
            case 2:
                WriteCheck(args_[0], args_[1]);
                break;
            case 3:
                DepositChecking(args_[0], args_[1]);
                break;
            case 4:
                TransactSaving(args_[1], args_[1]);
                break;
            case 5:
                SendPayment(args_[0], args_[1], args_[2]);
                break;
            case 6:
                Commit();
        }
        return null;
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
        Item acc_1 = db_.getState(SmallBankConstants.ACCOUNTS_TAB, acc1);
        Item acc_2 = db_.getState(SmallBankConstants.ACCOUNTS_TAB, acc2);
        // add to read set
        readSet_.put(SmallBankConstants.ACCOUNTS_TAB+"_"+acc1, acc_1);
        readSet_.put(SmallBankConstants.ACCOUNTS_TAB+"_"+acc2, acc_2);

        Item bal1 = db_.getState(SmallBankConstants.SAVINGS_TAB, acc1);
        Item bal2 = db_.getState(SmallBankConstants.CHECKINGS_TAB, acc2);
        int total = bal1.getValue_()+bal2.getValue_();
        // add to read set
        readSet_.put(SmallBankConstants.SAVINGS_TAB+"_"+acc1, bal1);
        readSet_.put(SmallBankConstants.CHECKINGS_TAB+"_"+acc2, bal2);

        // add to write set
        writeSet_.put(SmallBankConstants.SAVINGS_TAB+"_"+acc1, new Item(0, tranId_));
        writeSet_.put(SmallBankConstants.CHECKINGS_TAB+"_"+acc2, new Item(total, tranId_));
    }

    /**
     * The WriteCheck removes an amount from the customer's
     * checking account.
     * @param acc
     * @param amount
     */
    private void WriteCheck(int acc, int amount) {
        // get account
        Item acc_ = db_.getState(SmallBankConstants.ACCOUNTS_TAB, acc);
        //add to read set
        readSet_.put(SmallBankConstants.ACCOUNTS_TAB+"_"+acc, acc_);

        Item bal1 = db_.getState(SmallBankConstants.SAVINGS_TAB, acc);
        Item bal2 = db_.getState(SmallBankConstants.CHECKINGS_TAB, acc);
        int total = bal1.getValue_() + bal2.getValue_();
        // add to read set
        readSet_.put(SmallBankConstants.SAVINGS_TAB+"_"+acc, bal1);
        readSet_.put(SmallBankConstants.CHECKINGS_TAB+"_"+acc, bal2);

        // write check, add penality if overdraft
        if(total < amount) {
            writeSet_.put(SmallBankConstants.CHECKINGS_TAB+"_"+acc, new Item(amount - 1, tranId_));
        }else {
            writeSet_.put(SmallBankConstants.CHECKINGS_TAB+"_"+acc, new Item(amount, tranId_));
        }
    }

    private void DepositChecking(int acc, int amount) {
        // get account
        Item acc_ = db_.getState(SmallBankConstants.ACCOUNTS_TAB, acc);
        //add to read set
        readSet_.put(SmallBankConstants.ACCOUNTS_TAB+"_"+acc, acc_);

        Item bal = db_.getState(SmallBankConstants.CHECKINGS_TAB, acc);
        readSet_.put(SmallBankConstants.CHECKINGS_TAB+"_"+acc, bal);

        writeSet_.put(SmallBankConstants.CHECKINGS_TAB, new Item(bal.getValue_()+amount, tranId_));
    }

    private void TransactSaving(int acc, int amount) {
        // get account
        Item acc_ = db_.getState(SmallBankConstants.ACCOUNTS_TAB, acc);
        //add to read set
        readSet_.put(SmallBankConstants.ACCOUNTS_TAB+"_"+acc, acc_);

        Item bal = db_.getState(SmallBankConstants.SAVINGS_TAB, acc);
        readSet_.put(SmallBankConstants.SAVINGS_TAB+"_"+acc, bal);

        writeSet_.put(SmallBankConstants.CHECKINGS_TAB, new Item(bal.getValue_()+amount, tranId_));
    }

    private void SendPayment(int acc1, int acc2, int amount) {
        // get accounts
        Item acc_1 = db_.getState(SmallBankConstants.ACCOUNTS_TAB, acc1);
        Item acc_2 = db_.getState(SmallBankConstants.ACCOUNTS_TAB, acc2);
        // add to read set
        readSet_.put(SmallBankConstants.ACCOUNTS_TAB+"_"+acc1, acc_1);
        readSet_.put(SmallBankConstants.ACCOUNTS_TAB+"_"+acc2, acc_2);

        // get checking balance
        Item bal1 = db_.getState(SmallBankConstants.CHECKINGS_TAB, acc1);
        Item bal2 = db_.getState(SmallBankConstants.CHECKINGS_TAB, acc2);
        // add to read set
        readSet_.put(SmallBankConstants.SAVINGS_TAB+"_"+acc1, bal1);
        readSet_.put(SmallBankConstants.CHECKINGS_TAB+"_"+acc2, bal2);

        // add to write set
        writeSet_.put(SmallBankConstants.SAVINGS_TAB+"_"+acc1, new Item(bal1.getValue_()-amount, tranId_));
        writeSet_.put(SmallBankConstants.CHECKINGS_TAB+"_"+acc2, new Item(bal2.getValue_()+amount, tranId_));
    }

    private void RequestValidate() {

    }

    private void Commit() {

    }

    private void Reset() {
        readSet_.clear();
        writeSet_.clear();
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SmallBankProcedure that = (SmallBankProcedure) o;
        return tranId_ == that.tranId_;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tranId_);
    }
}