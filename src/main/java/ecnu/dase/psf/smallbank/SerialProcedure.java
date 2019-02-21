package ecnu.dase.psf.smallbank;

import ecnu.dase.psf.common.Item;
import ecnu.dase.psf.storage.DB;

import java.util.concurrent.Callable;

/**
 * @author MartyPang
 * @version 1.0
 * @date 2019/2/18 15:22
 */
public class SerialProcedure implements Callable<Long> {
    private DB db_;
    private int tranId_;

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
    private int[] randoms_;

    public SerialProcedure(DB db, int tranId) {
        db_ = db;
        tranId_ = tranId;
    }

    public void setParameters(int op, int[] args, int[] randoms) {
        op_ = op;
        args_ = args;
        randoms_ = randoms;
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
                TransactSaving(args_[0], args_[1]);
                break;
            case 5:
                SendPayment(args_[0], args_[1], args_[2]);
                break;
            case 6:
                break;
            default:
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
        Item acc_1 = db_.getState(SmallBankConstants.ACCOUNTS_TAB, acc1,randoms_[0]);
        Item acc_2 = db_.getState(SmallBankConstants.ACCOUNTS_TAB, acc2, randoms_[1]);

        Item bal1 = db_.getState(SmallBankConstants.SAVINGS_TAB, acc1, randoms_[2]);
        Item bal2 = db_.getState(SmallBankConstants.CHECKINGS_TAB, acc2, randoms_[3]);
        int total = bal1.getValue_()+bal2.getValue_();

        db_.putState(tranId_, SmallBankConstants.SAVINGS_TAB, acc1, 0, randoms_[4]);
        db_.putState(tranId_, SmallBankConstants.CHECKINGS_TAB, acc2, total, randoms_[5]);
    }

    /**
     * The WriteCheck removes an amount from the customer's
     * checking account.
     * @param acc
     * @param amount
     */
    private void WriteCheck(int acc, int amount) {
        // get account
        Item acc_ = db_.getState(SmallBankConstants.ACCOUNTS_TAB, acc, randoms_[0]);

        Item bal1 = db_.getState(SmallBankConstants.SAVINGS_TAB, acc, randoms_[1]);
        Item bal2 = db_.getState(SmallBankConstants.CHECKINGS_TAB, acc, randoms_[2]);
        int total = bal1.getValue_() + bal2.getValue_();

        // write check, add penality if overdraft
        if(total < amount) {
            db_.putState(tranId_, SmallBankConstants.CHECKINGS_TAB, acc, amount-1, randoms_[3]);
        }else {
            db_.putState(tranId_, SmallBankConstants.CHECKINGS_TAB, acc, amount, randoms_[3]);
        }
    }

    private void DepositChecking(int acc, int amount) {
        // get account
        Item acc_ = db_.getState(SmallBankConstants.ACCOUNTS_TAB, acc, randoms_[0]);
        Item bal = db_.getState(SmallBankConstants.CHECKINGS_TAB, acc, randoms_[1]);

        db_.putState(tranId_, SmallBankConstants.CHECKINGS_TAB, acc, bal.getValue_()+amount, randoms_[2]);
    }

    private void TransactSaving(int acc, int amount) {
        // get account
        Item acc_ = db_.getState(SmallBankConstants.ACCOUNTS_TAB, acc, randoms_[0]);
        Item bal = db_.getState(SmallBankConstants.SAVINGS_TAB, acc, randoms_[1]);

        db_.putState(tranId_, SmallBankConstants.CHECKINGS_TAB, acc, bal.getValue_()+amount, randoms_[2]);
    }

    private void SendPayment(int acc1, int acc2, int amount) {
        // get accounts
        Item acc_1 = db_.getState(SmallBankConstants.ACCOUNTS_TAB, acc1, randoms_[0]);
        Item acc_2 = db_.getState(SmallBankConstants.ACCOUNTS_TAB, acc2, randoms_[1]);

        // get checking balance
        Item bal1 = db_.getState(SmallBankConstants.CHECKINGS_TAB, acc1, randoms_[2]);
        Item bal2 = db_.getState(SmallBankConstants.CHECKINGS_TAB, acc2, randoms_[3]);

        db_.putState(tranId_, SmallBankConstants.SAVINGS_TAB, acc1, bal1.getValue_()-amount, randoms_[4]);
        db_.putState(tranId_, SmallBankConstants.CHECKINGS_TAB, acc2, bal2.getValue_()+amount, randoms_[5]);
    }

    public int getTranId_() {
        return tranId_;
    }

    public void setTranId_(int tranId_) {
        this.tranId_ = tranId_;
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
}
