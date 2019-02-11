package ecnu.dase.psf.concurrencycontrol;

import ecnu.dase.psf.common.Item;
import ecnu.dase.psf.common.Vertex;
import ecnu.dase.psf.smallbank.SmallBankConstants;
import ecnu.dase.psf.smallbank.SmallBankProcedure;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class BatchingOCC {
    private int nValidation; // num of transactions request validation
    private int nCommit; // num of transactions committed

    public BatchingOCC() {
        nValidation = 0;
        nCommit = 0;
    }

    public ConflictGraph ConstructCG(List<SmallBankProcedure> txs) {
        ConflictGraph cg = new ConflictGraph();
        // Add vertex for each transaction
        for(SmallBankProcedure tx : txs) {
            cg.addVertex(tx.getTranId_());
        }
        Map<Integer, Vertex> vertices = cg.getVertices();
        for(SmallBankProcedure tx : txs) {
            Map<String, Item> writeSet = tx.getWriteSet_();
            for(SmallBankProcedure otherTx : txs) {
                if(tx.equals(otherTx)) { //compare with other transaction's read set
                    continue;
                }
                if(hasConflict(writeSet, otherTx.getReadSet_())) {
                    /**
                     *         tx
                     *        /\
                     *        |
                     *       /
                     *      /
                     *     /
                     * otherTx
                     */
                    cg.addEdge(otherTx.getTranId_(), tx.getTranId_());
                }
            }
        }
        return cg;
    }

    /**
     * Check if t's read set has conflict with other t's write set
     * @param writeSet
     * @param readSet
     * @return true for having conflict
     *         false for not having any
     */
    private boolean hasConflict(Map<String, Item> writeSet, Map<String, Item> readSet) {
        if(null == writeSet || null == readSet) { // if one of them is null
            return  false;
        }
        boolean conflict = false;
        Set<String> readKeys = readSet.keySet();
        for(String key : readKeys) {
            if(writeSet.containsKey(key)) {
                conflict = true;
                break;
            }
        }
        return conflict;
    }

}
