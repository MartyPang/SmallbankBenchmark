package ecnu.dase.psf.concurrencycontrol;

import ecnu.dase.psf.smallbank.DeProcedure;
import ecnu.dase.psf.smallbank.DeSmallBank;

import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author MartyPang
 * @version 1.0
 * @date 2019/2/18 14:42
 */
public class Validator {
    private ExecutorService pool;
    private Stack<Integer> topologic;
    private List<DeProcedure> tasks;
    private Map<Integer, DeSmallBank> allTasks;

    public Validator(ExecutorService pool) {
        this.pool = pool;
    }

    public void concurrentValidate() {
        List<Future<Long>> futureList;
        try {
            futureList = pool.invokeAll(tasks, 1, TimeUnit.MINUTES);
            for(Future<Long> f : futureList) {
                f.get();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void shutdownPool() {
        pool.shutdown();
    }

    public List<DeProcedure> getTasks() {
        return tasks;
    }

    public void setTasks(List<DeProcedure> tasks) {
        this.tasks = tasks;
    }

    public Map<Integer, DeSmallBank> getAllTasks() {
        return allTasks;
    }

    public Stack<Integer> getTopologic() {
        return topologic;
    }

    public void setTopologic(Stack<Integer> topologic) {
        this.topologic = topologic;
    }

    public void setCommitProcedure() {
        DeProcedure commit = new DeProcedure();
        commit.setCommit(true);
        commit.setTopo(topologic);
        commit.setAllTxs(allTasks);
        tasks.add(commit);
    }

    public void setAllTasks(Map<Integer, DeSmallBank> allTasks) {
        this.allTasks = allTasks;

    }

    public void reset() {
        //topologic.clear();
        //tasks.clear();
        //commitTask.clear();
        //allTasks.clear();
    }
}
