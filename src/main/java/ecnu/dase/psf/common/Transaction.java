package ecnu.dase.psf.common;

import java.util.HashMap;

public class Transaction {
    /**
     * Transaction info
     */
    protected int tranId_;
    /**
     * Key: TableName_Index
     * Value: read value
     */
    protected HashMap<String, Integer> readSet_;
    /**
     * Key: TableName_Index
     * Value: write value
     */
    protected HashMap<String, Integer> writeSet_;
}
