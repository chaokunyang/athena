package com.timeyang.athena.scheduler;

import com.timeyang.athena.task.TaskInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author https://github.com/chaokunyang
 */
public class Job {
    private List<TaskInfo> tasks = new ArrayList<>();
    private List<Node> nodes;

    public Job addTask(TaskInfo task) {
        tasks.add(task);
        return this;
    }

    public Job addTask(TaskInfo... tasks) {
        this.tasks.addAll(Arrays.asList(tasks));
        return this;
    }

    public Job setUpStrteam(TaskInfo taskInfo, TaskInfo up) {
        nodes.add(new Node(taskInfo, up));
        return this;
    }

    public void buildGrapth() {

    }

    List<TaskInfo> getTasks() {
        return tasks;
    }

    List<Node> getNodes() {
        return nodes;
    }
}

class Node {
    private TaskInfo taskInfo;
    private TaskInfo up;

    Node(TaskInfo taskInfo, TaskInfo up) {
        this.taskInfo = taskInfo;
        this.up = up;
    }

    public TaskInfo getTaskInfo() {
        return taskInfo;
    }

    public TaskInfo getUp() {
        return up;
    }
}
