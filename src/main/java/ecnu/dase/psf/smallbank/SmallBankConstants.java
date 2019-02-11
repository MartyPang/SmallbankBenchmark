package ecnu.dase.psf.smallbank;

public abstract class SmallBankConstants {
    /**
     * TABLE NAMES
     */
    public static final String ACCOUNTS_TAB = "accounts";
    public static final String SAVINGS_TAB = "savings";
    public static final String CHECKINGS_TAB = "checkings";

    public static final int BATCH_SIZE = 400;

    /**
     * ACCOUNT INFORMATION
     */
    public static final int NUM_ACCOUNTS = 1000000;

    public static final boolean HOTSPOT_USE_FIXED_SIZE  = false;
    public static final double HOTSPOT_PERCENTAGE       = 25; // [0% - 100%]
    public static final int HOTSPOT_FIXED_SIZE          = 100; // fixed number of tuples

    public static final int MAX_BALANCE = 50000;
    public static final int MIN_BALANCE = 10000;

    /**
     * EXECUTION CONFIG
     */
    public static  final int DEFAULT_THREAD = 4;

}
