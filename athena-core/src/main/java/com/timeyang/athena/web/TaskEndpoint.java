package com.timeyang.athena.web;

import com.timeyang.athena.Athena;
import com.timeyang.athena.task.TaskInfo.WaitingTask;
import com.timeyang.athena.task.TaskManager;
import com.timeyang.athena.utill.jdbc.Page;
import com.timeyang.athena.utill.jdbc.PagedResult;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import static com.timeyang.athena.task.TaskInfo.FinishedTask;
import static com.timeyang.athena.task.TaskInfo.RunningTask;

/**
 * @author https://github.com/chaokunyang
 */
@Path("/task")
public class TaskEndpoint {
    private TaskManager taskManager = Athena.getInstance().getTaskManager();

    @POST
    @Path("/waiting_tasks")
    @Produces(MediaType.APPLICATION_JSON)
    public PagedResult<WaitingTask> getWaitingTasks(Page page) {
        return taskManager.getWaitingTasks(page);
    }

    @POST
    @Path("/running_tasks")
    @Produces(MediaType.APPLICATION_JSON)
    public PagedResult<RunningTask> getRunningTasks(Page page) {
        return taskManager.getRunningTasks(page);
    }

    @POST
    @Path("/finished_tasks")
    @Produces(MediaType.APPLICATION_JSON)
    public PagedResult<FinishedTask> getFinishedTasks(Page page) {
        return taskManager.getFinishedTasks(page);
    }
}
