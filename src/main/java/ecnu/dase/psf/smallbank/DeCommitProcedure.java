package ecnu.dase.psf.smallbank;

import java.util.Map;
import java.util.Stack;
import java.util.concurrent.Callable;

/**
 * @author MartyPang
 * @version 1.0
 * @date 2019/2/18 20:44
 */
public class DeCommitProcedure  implements Callable<Long> {
    private Map<Integer, DeSmallBank> allTx;
    private Stack<Integer> topologic;

    public DeCommitProcedure() {

    }

    public DeCommitProcedure(Map<Integer, DeSmallBank> txs, Stack<Integer> topo) {
        allTx = txs;
        topologic = topo;
    }

    @Override
    public Long call() throws Exception {
        while(!topologic.empty()) {
            int tranId = topologic.pop();
            DeSmallBank current = allTx.get(tranId);
            while(!current.isCommit()) {
                System.out.println("not commit");
            }
            current.Commit();
        }
        return null;
    }

    public Map<Integer, DeSmallBank> getAllTx() {
        return allTx;
    }

    public void setAllTx(Map<Integer, DeSmallBank> allTx) {
        this.allTx = allTx;
    }

    public Stack<Integer> getTopologic() {
        return topologic;
    }

    public void setTopologic(Stack<Integer> topologic) {
        this.topologic = topologic;
    }
}
