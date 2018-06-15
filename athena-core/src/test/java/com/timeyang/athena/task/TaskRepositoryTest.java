package com.timeyang.athena.task;

import com.timeyang.athena.Athena;
import com.timeyang.athena.util.jdbc.Criterion;
import com.timeyang.athena.util.jdbc.Page;
import com.timeyang.athena.util.jdbc.PagedResult;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

public class TaskRepositoryTest {

    private TaskRepository taskRepository;

    @Before
    public void setUp() {
        Athena athena = Athena.builder()
                .disable("hive", "webServer", "messageServer")
                .getOrCreate();
        taskRepository = new TaskRepository(athena.getDataSource());
    }

    @Test
    public void createWaitingTasks() {
        for (int i = 0; i < 20; i++) {
            TaskInfo waitingTask = new TaskInfo.WaitingTask();
            waitingTask.setTaskName("task" + i + System.currentTimeMillis());
            waitingTask.setHost("localhost");
            waitingTask.setClassName("com.timeyang.athena.Test");
            waitingTask.setParams("--date 1970/01/01");
            waitingTask.setMaxTries(i % 5);
            waitingTask.setRetryWait((long) (i %  10));
            waitingTask.setTaskType(TaskType.JAVA);
            waitingTask.setSubmitTime(Instant.now());

            waitingTask = taskRepository.create(waitingTask);
            System.out.println(waitingTask);
        }
    }

    @Test
    public void getWaitingTasks() {
        createWaitingTasks();

        PagedResult<TaskInfo.WaitingTask> tables = taskRepository.getWaitingTasks(new Page(0, 5));
        System.out.println(tables);
        List<Page.Sort> sorts = Arrays.asList(
                new Page.Sort("submit_time", Page.Order.DESC),
                new Page.Sort("task_name", Page.Order.ASC));
        List<Criterion> criteria = Arrays.asList(
                new Criterion("host", Criterion.Predicate.EQ, "localhost"),
                new Criterion("class_name", Criterion.Predicate.LIKE, "athena"),
                new Criterion("retry_wait", Criterion.Predicate.NEQ, 3),
                new Criterion("max_tries", Criterion.Predicate.EQ, 1));

        PagedResult<TaskInfo.WaitingTask> sortedTasks = taskRepository.getWaitingTasks(new Page(0, 5, sorts, criteria));
        System.out.printf("--------Sorted tasks--------: total size [%d], page [%d], size[%d] \n",
                sortedTasks.getTotalSize(), sortedTasks.getPage(), sortedTasks.getSize());
        sortedTasks.getElements().forEach(System.out::println);
    }

}