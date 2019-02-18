package ecnu.dase.psf.smallbank;

import ecnu.dase.psf.common.Item;
import ecnu.dase.psf.storage.HybridDB;

import java.util.HashMap;
import java.util.Map;

/**
 * @author MartyPang
 * @version 1.0
 * @date 2019/2/14 10:40
 */
public class DeSmallBank {
    private int tranId_;
    private HybridDB hdb_;
    private boolean commit;
    /**
     * Consistent read set
     */
    //private Map<String, Item> R;
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

    public DeSmallBank(HybridDB db, int tranId) {
        hdb_ = db;
        tranId_ = tranId;
        commit = false;
        readSet_ = new HashMap<>();
        writeSet_ = new HashMap<>();
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
    public Long run() throws Exception {
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
                TransactSaving(args_[0], args_[1]);
                break;
            case 5:
                SendPayment(args_[0], args_[1], args_[2]);
                break;
            case 6:
                break;
            default:
        }
        //update consistent read set
        //hdb_.updateR(tranId_, writeSet_);
        commit = true;
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
        Item acc_1 = hdb_.getState(tranId_, SmallBankConstants.ACCOUNTS_TAB, acc1);
        Item acc_2 = hdb_.getState(tranId_, SmallBankConstants.ACCOUNTS_TAB, acc2);
        // add to read set
        readSet_.put(SmallBankConstants.ACCOUNTS_TAB+"_"+acc1, acc_1);
        readSet_.put(SmallBankConstants.ACCOUNTS_TAB+"_"+acc2, acc_2);

        Item bal1 = hdb_.getState(tranId_, SmallBankConstants.SAVINGS_TAB, acc1);
        Item bal2 = hdb_.getState(tranId_, SmallBankConstants.CHECKINGS_TAB, acc2);
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
        Item acc_ = hdb_.getState(tranId_, SmallBankConstants.ACCOUNTS_TAB, acc);
        //add to read set
        readSet_.put(SmallBankConstants.ACCOUNTS_TAB+"_"+acc, acc_);

        Item bal1 = hdb_.getState(tranId_, SmallBankConstants.SAVINGS_TAB, acc);
        Item bal2 = hdb_.getState(tranId_, SmallBankConstants.CHECKINGS_TAB, acc);
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
        Item acc_ = hdb_.getState(tranId_, SmallBankConstants.ACCOUNTS_TAB, acc);
        //add to read set
        readSet_.put(SmallBankConstants.ACCOUNTS_TAB+"_"+acc, acc_);

        Item bal = hdb_.getState(tranId_, SmallBankConstants.CHECKINGS_TAB, acc);
        readSet_.put(SmallBankConstants.CHECKINGS_TAB+"_"+acc, bal);

        writeSet_.put(SmallBankConstants.CHECKINGS_TAB+"_"+acc, new Item(bal.getValue_()+amount, tranId_));
    }

    private void TransactSaving(int acc, int amount) {
        // get account
        Item acc_ = hdb_.getState(tranId_, SmallBankConstants.ACCOUNTS_TAB, acc);
        //add to read set
        readSet_.put(SmallBankConstants.ACCOUNTS_TAB+"_"+acc, acc_);

        Item bal = hdb_.getState(tranId_, SmallBankConstants.SAVINGS_TAB, acc);
        readSet_.put(SmallBankConstants.SAVINGS_TAB+"_"+acc, bal);

        writeSet_.put(SmallBankConstants.CHECKINGS_TAB+"_"+acc, new Item(bal.getValue_()+amount, tranId_));
    }

    private void SendPayment(int acc1, int acc2, int amount) {
        // get accounts
        Item acc_1 = hdb_.getState(tranId_, SmallBankConstants.ACCOUNTS_TAB, acc1);
        Item acc_2 = hdb_.getState(tranId_, SmallBankConstants.ACCOUNTS_TAB, acc2);
        // add to read set
        readSet_.put(SmallBankConstants.ACCOUNTS_TAB+"_"+acc1, acc_1);
        readSet_.put(SmallBankConstants.ACCOUNTS_TAB+"_"+acc2, acc_2);

        // get checking balance
        Item bal1 = hdb_.getState(tranId_, SmallBankConstants.CHECKINGS_TAB, acc1);
        Item bal2 = hdb_.getState(tranId_, SmallBankConstants.CHECKINGS_TAB, acc2);
        // add to read set
        readSet_.put(SmallBankConstants.SAVINGS_TAB+"_"+acc1, bal1);
        readSet_.put(SmallBankConstants.CHECKINGS_TAB+"_"+acc2, bal2);

        // add to write set
        writeSet_.put(SmallBankConstants.SAVINGS_TAB+"_"+acc1, new Item(bal1.getValue_()-amount, tranId_));
        writeSet_.put(SmallBankConstants.CHECKINGS_TAB+"_"+acc2, new Item(bal2.getValue_()+amount, tranId_));
    }
    public void Commit() {
        String table;
        int acc = 0;
        for(String key : writeSet_.keySet()) {
            String[] args = key.split("_");
            table = args[0];
            acc = Integer.parseInt(args[1]);
            hdb_.putState(tranId_, table, acc, writeSet_.get(key).getValue_());
        }
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

    public boolean isCommit() {
        return commit;
    }

    public void setCommit(boolean commit) {
        this.commit = commit;
    }
}
