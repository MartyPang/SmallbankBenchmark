package ecnu.dase.psf.concurrencycontrol;

import ecnu.dase.psf.smallbank.SerialProcedure;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author MartyPang
 * @version 1.0
 * @date 2019/2/18 19:04
 */
public class SerialRunner {
    private List<SerialProcedure> serialTasks;
    private ExecutorService serialPool;

    public SerialRunner() {
        serialPool = Executors.newSingleThreadExecutor();
    }

    public void serialExecute() {
        List<Future<Long>> futureList;
        try {
            futureList = serialPool.invokeAll(serialTasks, 5, TimeUnit.MINUTES);
            for(Future<Long> f : futureList) {
                f.get();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void shutdownPool() {
        serialPool.shutdown();
    }

    public List<SerialProcedure> getSerialTasks() {
        return serialTasks;
    }

    public void setSerialTasks(List<SerialProcedure> serialTasks) {
        this.serialTasks = serialTasks;
    }
}
