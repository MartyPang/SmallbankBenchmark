package ecnu.dase.psf.storage;

import ecnu.dase.psf.common.Item;
import ecnu.dase.psf.smallbank.SmallBankConstants;

import java.util.concurrent.ThreadLocalRandom;

public class DB {
    Item[] account_;
    Item[] saving_;
    Item[] checking_;

    ThreadLocalRandom localR = ThreadLocalRandom.current();

    public DB(int num_accounts, int balance) {
        account_ = new Item[num_accounts];
        saving_ = new Item[num_accounts];
        checking_ = new Item[num_accounts];

        for(int i=0; i<num_accounts; ++i) {
            account_[i] = new Item(i, 0);
            saving_[i] = new Item(balance, 0);
            checking_[i] = new Item(balance, 0);
        }
    }

    public Item getState(String table, int acc, int internal) {
        //simulate processing time
        for(int i = 0;i<20;++i){
            for(int j = 0;j<internal;++j){
                isPrime(i*j);
            }
        }
        Item value = null;
        if(table.equals(SmallBankConstants.ACCOUNTS_TAB)) {
            value = account_[acc];
        }
        else if(table.equals(SmallBankConstants.SAVINGS_TAB)) {
            value = saving_[acc];
        }else if(table.equals(SmallBankConstants.CHECKINGS_TAB)) {
            value = checking_[acc];
        }
        return value;
    }

    public void putState(int writtenBy, String table, int acc, int value, int internal) {
        for(int i = 0;i<20;++i){
            for(int j = 0;j<internal;++j){
                isPrime(i*j);
            }
        }
        if(table.equals(SmallBankConstants.SAVINGS_TAB)) {
            saving_[acc].setWrittenBy_(writtenBy);
            saving_[acc].setValue_(value);
        }else if(table.equals(SmallBankConstants.CHECKINGS_TAB)) {
            checking_[acc].setWrittenBy_(writtenBy);
            checking_[acc].setValue_(value);
        }
    }

    public void putState(int writtenBy, String table, int acc, int value) {
        if(table.equals(SmallBankConstants.SAVINGS_TAB)) {
            saving_[acc].setWrittenBy_(writtenBy);
            saving_[acc].setValue_(value);
        }else if(table.equals(SmallBankConstants.CHECKINGS_TAB)) {
            checking_[acc].setWrittenBy_(writtenBy);
            checking_[acc].setValue_(value);
        }
    }

    public boolean isPrime(int a) {
        boolean flag = true;
        if (a < 2) {// 素数不小于2
            return false;
        } else {
            for (int i = 2; i <= Math.sqrt(a); i++) {
                if (a % i == 0) {// 若能被整除，则说明不是素数，返回false
                    flag = false;
                    //break;// 跳出循环
                }
            }
        }
        return flag;
    }
}
