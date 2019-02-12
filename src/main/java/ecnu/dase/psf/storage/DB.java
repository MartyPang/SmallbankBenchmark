package ecnu.dase.psf.storage;

import ecnu.dase.psf.common.Item;
import ecnu.dase.psf.smallbank.SmallBankConstants;

public class DB {
    Item[] account_;
    Item[] saving_;
    Item[] checking_;

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

    public Item getState(String table, int acc) {
        Item value = null;
        if(table.equals(SmallBankConstants.SAVINGS_TAB)) {
            value = saving_[acc];
        }else if(table.equals(SmallBankConstants.CHECKINGS_TAB)) {
            value = checking_[acc];
        }
        return value;
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
}
