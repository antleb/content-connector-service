package eu.openminted.content.service.threads;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/***
 * A wrapper class for initialising and managing a Threadpool
 */

public class ThreadpoolWrapper {

    private static Logger logger = Logger.getLogger(ThreadpoolWrapper.class);

    private ExecutorService executor;
    private List<Callable<String>> tasksQueue = new ArrayList<>();
    private List<String> results = new ArrayList<>();

    public ThreadpoolWrapper(ExecutorService executor) {
        this.executor = executor;
    }

    /***
     * Setters and getters
     */

    public ExecutorService getExecutor() {
        return executor;
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public List<Callable<String>> getTasksQueue() {
        return tasksQueue;
    }

    public void setTasksQueue(List<Callable<String>> tasksQueue) {
        this.tasksQueue = tasksQueue;
    }

    public List<String> getResults() {
        return results;
    }

    public void setResults(List<String> results) {
        this.results = results;
    }

    /**
     *  Adds a task to the task queue
     */
    public void addTask(Callable<String> task) {
        logger.debug("Adding task " + task.getClass());
        tasksQueue.add(task);
    }

    /**
     *  Runs (invokes) all tasks, waiting for a specific timeout(in seconds).
     *  Gets the results of the (uninterrupted) thread jobs and adds them to the results list
     */
    public void invokeAll(int timeout) {

        try {
            List<Future<String>> futures = executor.invokeAll(tasksQueue, timeout, TimeUnit.SECONDS);
            for (Future<String> future : futures) {
                if (!future.isCancelled()) {
                    try {
                        results.add(future.get());
                    } catch (ExecutionException e) {
                        logger.error("Error invoking task", e);
                    }
                }
            }
        } catch (InterruptedException e) {
            logger.error("Task interrupted", e);
        }
    }

    public void shutdown() {
        logger.debug("Shuting down threadpool");
        executor.shutdown();
    }
}
