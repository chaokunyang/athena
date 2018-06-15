package com.timeyang.athena.task;

import com.timeyang.athena.AthenaConf;
import com.timeyang.athena.task.TaskInfo.FinishedTask;
import com.timeyang.athena.task.TaskInfo.RunningTask;
import com.timeyang.athena.task.TaskInfo.WaitingTask;
import com.timeyang.athena.task.exec.TaskFactory;
import com.timeyang.athena.task.scheduler.TaskScheduler;
import com.timeyang.athena.task.scheduler.TaskSchedulerImpl;
import com.timeyang.athena.util.jdbc.Page;
import com.timeyang.athena.util.jdbc.PagedResult;
import com.timeyang.athena.util.ParametersUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;

/**
 * task manager
 *
 * @author https://github.com/chaokunyang
 */
public class TaskManagerImpl implements TaskManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskManagerImpl.class);

    private final AthenaConf athenaConf;
    private final TaskRepository taskRepository;

    private final TaskScheduler taskScheduler;

    public TaskManagerImpl(AthenaConf athenaConf, DataSource dataSource) {
        this.athenaConf = athenaConf;
        this.taskRepository = new TaskRepository(dataSource);
        this.taskScheduler = new TaskSchedulerImpl(athenaConf, taskRepository);
    }

    @Override
    public void start() {
        taskScheduler.start();
        LOGGER.info("Task manager started");
    }

    @Override
    public void stop() {
        taskScheduler.stop();
        LOGGER.info("Task manager stopped");
    }

    @Override
    public long submitTask(String taskName, String className, Map<String, String> params) {
        TaskInfo taskInfo = new WaitingTask();
        taskInfo.setTaskName(taskName);
        taskInfo.setClassName(className);
        taskInfo.setParams(ParametersUtils.toArgs(params));
        taskInfo.setMaxTries(athenaConf.getDefaultTaskRetryNumber());
        taskInfo.setRetryWait(athenaConf.getDefaultTaskRetryWait());

        return submitTask(taskInfo);
    }

    @Override
    public long submitTask(String taskName, Class<? extends TaskFactory> factoryClass, Map<String, String> params) {
        TaskInfo taskInfo = new WaitingTask();
        taskInfo.setTaskName(taskName);
        taskInfo.setClassName(factoryClass.getCanonicalName());
        taskInfo.setParams(ParametersUtils.toArgs(params));
        taskInfo.setMaxTries(athenaConf.getDefaultTaskRetryNumber());
        taskInfo.setRetryWait(athenaConf.getDefaultTaskRetryWait());

        return submitTask(taskInfo);
    }

    @Override
    public long submitTask(String taskName, Class<? extends TaskFactory> factoryClass, Map<String, String> params, int maxTries) {
        TaskInfo taskInfo = new WaitingTask();
        taskInfo.setTaskName(taskName);
        taskInfo.setClassName(factoryClass.getCanonicalName());
        taskInfo.setParams(ParametersUtils.toArgs(params));
        taskInfo.setMaxTries(maxTries);
        taskInfo.setRetryWait(athenaConf.getDefaultTaskRetryWait());

        return submitTask(taskInfo);
    }

    @Override
    public long submitTask(String taskName, Class<? extends TaskFactory> factoryClass, Map<String, String> params, int maxTries, long retryWait) {
        TaskInfo taskInfo = new WaitingTask();
        taskInfo.setTaskName(taskName);
        taskInfo.setClassName(factoryClass.getCanonicalName());
        taskInfo.setParams(ParametersUtils.toArgs(params));
        taskInfo.setMaxTries(maxTries);
        taskInfo.setRetryWait(retryWait);

        return submitTask(taskInfo);
    }

    @Override
    public long submitTask(TaskInfo task) {
        if (task.getMaxTries() == null)
            task.setMaxTries(athenaConf.getDefaultTaskRetryNumber());
        if (task.getRetryWait() == null)
            task.setRetryWait(athenaConf.getDefaultTaskRetryWait());
        if (task.getTaskType() == null)
            task.setTaskType(TaskType.JAVA);
        task.setSubmitTime(Instant.now());

        return this.taskRepository.create(task).getTaskId();
    }

    @Override
    public Future killTask(long taskId) {
        return this.taskScheduler.killTask(taskId);
    }

    @Override
    public boolean isTaskAlive(long taskId) {
        return this.taskScheduler.isTaskRunning(taskId);
    }

    @Override
    public boolean isTaskFinished(long taskId) {
        FinishedTask finishedTask = this.taskRepository.getFinishedTask(taskId);
        // return true for if task finished or not exists
        boolean isFinished = finishedTask != null ||
                (this.taskRepository.getWaitingTask(taskId) == null && !this.taskRepository.getRunningTask(taskId).isPresent());

        LOGGER.info("task [{}] is finished: {}", isFinished);
        return isFinished;
    }

    @Override
    public List<String> getLogLines(long taskId, int lineNumber, int rows) {
        return this.taskScheduler.getLogLines(taskId, lineNumber, rows);
    }

    @Override
    public TaskInfo getTask(long taskId) {
        WaitingTask waitingTask = taskRepository.getWaitingTask(taskId);
        if (waitingTask != null) {
            return waitingTask;
        }
        Optional<RunningTask> runningTaskOptional = taskRepository.getRunningTask(taskId);
        if (runningTaskOptional.isPresent()) {
            return runningTaskOptional.get();
        }
        return taskRepository.getFinishedTask(taskId);
    }

    @Override
    public PagedResult<WaitingTask> getWaitingTasks(Page page) {
        return taskRepository.getWaitingTasks(page);
    }

    @Override
    public PagedResult<RunningTask> getRunningTasks(Page page) {
        return taskRepository.getRunningTasks(page);
    }

    @Override
    public PagedResult<FinishedTask> getFinishedTasks(Page page) {
        return taskRepository.getFinishedTasks(page);
    }

}
