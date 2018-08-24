package com.timeyang.athena.task;

import java.time.Duration;
import java.time.Instant;

/**
 * @author https://github.com/chaokunyang
 */

public abstract class TaskInfo {
    private Long taskId;
    private String taskName;
    private TaskType taskType;
    private String host;
    /**
     * <p>If host fixed, host must supplied by task submitter and changing host by taskScheduler is not allowed. Mainly for Daemon task.</p>
     * <p>If not fixed, host is only an advised task execution host</p>
     */
    private boolean hostFixed;
    /**
     * {@link com.timeyang.athena.task.exec.Task} className or <br/>
     * {@link com.timeyang.athena.task.exec.TaskFactory} className
     */
    private String className;
    private String libs;
    private String params;
    /**
     * max retry number
     */
    private Integer maxTries;
    /**
     * retry wait seconds
     */
    private Long retryWait;
    private Instant submitTime;

    public Long getTaskId() {
        return taskId;
    }

    public void setTaskId(Long taskId) {
        this.taskId = taskId;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public void setTaskType(TaskType taskType) {
        this.taskType = taskType;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public boolean isHostFixed() {
        return hostFixed;
    }

    public void setHostFixed(boolean hostFixed) {
        this.hostFixed = hostFixed;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getLibs() {
        return libs;
    }

    public void setLibs(String libs) {
        this.libs = libs;
    }

    public String getParams() {
        return params;
    }

    public void setParams(String params) {
        this.params = params;
    }

    public Integer getMaxTries() {
        return maxTries;
    }

    public void setMaxTries(Integer maxTries) {
        this.maxTries = maxTries;
    }

    public Long getRetryWait() {
        return retryWait;
    }

    public void setRetryWait(Long retryWait) {
        this.retryWait = retryWait;
    }

    public Instant getSubmitTime() {
        return submitTime;
    }

    public void setSubmitTime(Instant submitTime) {
        this.submitTime = submitTime;
    }

    @Override
    public String toString() {
        return "taskId=" + taskId +
                ", taskName='" + taskName + '\'' +
                ", taskType='" + taskType + '\'' +
                ", host='" + host + '\'' +
                ", hostFixed='" + hostFixed + '\'' +
                ", className='" + className + '\'' +
                ", libs='" + libs + '\'' +
                ", params='" + params + '\'' +
                ", maxTries=" + maxTries +
                ", retryWait=" + retryWait +
                ", submitTime=" + submitTime;
    }

    public static final class WaitingTask extends TaskInfo {}

    public static final class RunningTask extends TaskInfo {

        private Instant startTime;
        private Integer tryNumber;
        private Integer pid;

        public RunningTask() { }

        public RunningTask(TaskInfo task) {
            super.setTaskId(task.getTaskId());
            super.setTaskName(task.getTaskName());
            super.setTaskType(task.getTaskType());
            super.setHost(task.getHost());
            super.setHostFixed(task.isHostFixed());
            super.setClassName(task.getClassName());
            super.setLibs(task.getLibs());
            super.setParams(task.getParams());
            super.setMaxTries(task.getMaxTries());
            super.setRetryWait(task.getRetryWait());
            super.setSubmitTime(task.getSubmitTime());
        }

        public Instant getStartTime() {
            return startTime;
        }

        public void setStartTime(Instant startTime) {
            this.startTime = startTime;
        }

        public Integer getTryNumber() {
            return tryNumber;
        }

        public void setTryNumber(Integer tryNumber) {
            this.tryNumber = tryNumber;
        }

        public Integer getPid() {
            return pid;
        }

        public void setPid(Integer pid) {
            this.pid = pid;
        }

        @Override
        public String toString() {
            return super.toString() +
                    ", startTime=" + startTime +
                    ", tryNumber=" + tryNumber +
                    ", pid='" + pid;
        }
    }

    public static final class FinishedTask extends TaskInfo {
        private Instant startTime;
        private Instant endTime;
        private Duration duration;
        private TaskState state;
        private Integer tryNumber;

        public FinishedTask() {}

        public FinishedTask(TaskInfo task) {
            super.setTaskId(task.getTaskId());
            super.setTaskName(task.getTaskName());
            super.setTaskType(task.getTaskType());
            super.setHost(task.getHost());
            super.setHostFixed(task.isHostFixed());
            super.setClassName(task.getClassName());
            super.setLibs(task.getLibs());
            super.setParams(task.getParams());
            super.setMaxTries(task.getMaxTries());
            super.setRetryWait(task.getRetryWait());
            super.setSubmitTime(task.getSubmitTime());
        }

        public FinishedTask(RunningTask task) {
            super.setTaskId(task.getTaskId());
            super.setTaskName(task.getTaskName());
            super.setTaskType(task.getTaskType());
            super.setHost(task.getHost());
            super.setHostFixed(task.isHostFixed());
            super.setClassName(task.getClassName());
            super.setLibs(task.getLibs());
            super.setParams(task.getParams());
            super.setMaxTries(task.getMaxTries());
            super.setRetryWait(task.getRetryWait());
            super.setSubmitTime(task.getSubmitTime());

            this.startTime = task.getStartTime();
            this.tryNumber = task.getTryNumber();

            this.endTime = Instant.now();
            this.duration = Duration.between(this.startTime, this.endTime);
        }

        public Instant getStartTime() {
            return startTime;
        }

        public void setStartTime(Instant startTime) {
            this.startTime = startTime;
        }

        public Instant getEndTime() {
            return endTime;
        }

        public void setEndTime(Instant endTime) {
            this.endTime = endTime;
        }

        public Duration getDuration() {
            return duration;
        }

        public void setDuration(Duration duration) {
            this.duration = duration;
        }

        public TaskState getState() {
            return state;
        }

        public void setState(TaskState state) {
            this.state = state;
        }

        public Integer getTryNumber() {
            return tryNumber;
        }

        public void setTryNumber(Integer tryNumber) {
            this.tryNumber = tryNumber;
        }

        @Override
        public String toString() {
            return super.toString() +
                    ", startTime=" + startTime +
                    ", endTime=" + endTime +
                    ", duration=" + duration +
                    ", state=" + state +
                    ", tryNumber=" + tryNumber;
        }
    }

}
