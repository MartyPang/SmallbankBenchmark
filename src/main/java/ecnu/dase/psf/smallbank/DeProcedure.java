package ecnu.dase.psf.smallbank;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.Callable;

/**
 * @author MartyPang
 * @version 1.0
 * @date 2019/2/18 9:50
 */
public class DeProcedure implements Callable<Long> {
    List<DeSmallBank> tasks;
    boolean isCommit;
    Stack<Integer> topo;
    Map<Integer, DeSmallBank> allTxs;
    //List<Integer> order;
    //DirectedGraph part;

    public DeProcedure() {
        tasks = new ArrayList<>();
        isCommit = false;
        //order = new ArrayList<>();
    }

    public DeProcedure(List<DeSmallBank> workload) {
        tasks = workload;
        isCommit = false;
    }

    @Override
    public Long call() throws Exception {
        Long start = System.currentTimeMillis();
        if(isCommit) {
            while(!topo.empty()) {
                int tranId = topo.pop();
                DeSmallBank current = allTxs.get(tranId);
                if(!current.isCommit()){

                }
                current.Commit();
            }
        }
        else {
            for(DeSmallBank sub : tasks) {
                sub.run();
            }
        }
        Long end =  System.currentTimeMillis();
        return end - start;
    }

    public List<DeSmallBank> getTasks() {
        return tasks;
    }

    public void setTasks(List<DeSmallBank> tasks) {
        this.tasks = tasks;
    }

    public boolean isCommit() {
        return isCommit;
    }

    public void setCommit(boolean commit) {
        isCommit = commit;
    }

    public Stack<Integer> getTopo() {
        return topo;
    }

    public void setTopo(Stack<Integer> topo) {
        this.topo = topo;
    }

    public Map<Integer, DeSmallBank> getAllTxs() {
        return allTxs;
    }

    public void setAllTxs(Map<Integer, DeSmallBank> allTxs) {
        this.allTxs = allTxs;
    }
}
