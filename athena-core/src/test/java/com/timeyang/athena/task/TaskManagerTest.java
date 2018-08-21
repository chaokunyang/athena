package com.timeyang.athena.task;

import com.timeyang.athena.Athena;
import com.timeyang.athena.AthenaConf;
import com.timeyang.athena.task.exec.TaskUtils;
import com.timeyang.athena.util.ClassUtils;
import com.timeyang.athena.util.IoUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class TaskManagerTest {

    private TaskManager taskManager;

    @Before
    public void setUp() {
        Athena athena = Athena.builder()
                .disable("hive", "webServer", "messageServer")
                .getOrCreate();
        AthenaConf athenaConf = athena.athenaConf();
        taskManager = new TaskManagerImpl(athenaConf, athena.getDataSource());
        taskManager.start();
    }

    @Test
    public void executeTask() throws InterruptedException, IOException {
        List<Long> taskIds = new ArrayList<>();
        int tasks = 3;

        for (int i = 0; i < tasks; i++) {
            String classpath = ClassUtils.getCurrentClasspath().stream().collect(Collectors.joining(";"));
            String athenaClasspath = Arrays.stream(classpath.split(";"))
                    .filter(cp -> cp.contains("athena"))
                    .collect(Collectors.joining(";"));
            System.out.println(athenaClasspath);

            String classpathFilePath = Paths
                    .get(TaskUtils.getTasksDir(), "classpathFile", String.valueOf(System.currentTimeMillis()))
                    .toAbsolutePath()
                    .toString();

            File classpathFile = new File(classpathFilePath);
            if (!classpathFile.getParentFile().exists()) {
                classpathFile.getParentFile().mkdirs();
            }
            IoUtils.writeFile(classpath, classpathFile);
            System.out.println("classpathFilePath: " + classpathFilePath);
            String params = "--classpathFile " + classpathFilePath;

            if (i == 2) {
                Map<String, String> paramsMap = new HashMap<>();
                paramsMap.put("extraClasspath", athenaClasspath);
                paramsMap.put("classpathFile", classpathFilePath);
                long taskId = taskManager.submitTask("TestTaskFactory" + System.currentTimeMillis(),
                        TestTaskFactory.class, paramsMap);
                System.out.println(taskId);
                continue;
            }

            TaskInfo task = new TaskInfo.WaitingTask();
            task.setTaskName("task" + System.currentTimeMillis());
            task.setHost("localhost");
            if (i % 3 == 0) {
                task.setClassName("com.timeyang.athena.task.TestTask");
            } else if (i % 3 == 1) {
                task.setClassName("com.timeyang.athena.task.TestFailTask");
            }
            task.setMaxTries(3);

            task.setClasspath(athenaClasspath);
            task.setParams(params);

            long taskId = taskManager.submitTask(task);
            taskIds.add(taskId);
            System.out.println(taskId);
        }

        Random random = new Random();
        Runnable f = () -> {
            try {
                Thread.sleep( 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long taskId = taskIds.get(random.nextInt(tasks - 1));
            List<String> lines = taskManager.getLogLines(taskId, 1, 10);
            System.out.println("********************** lines start **********************");
            lines.forEach(System.out::println);
            System.out.println("********************** lines end **********************");
        };
        while (taskIds.stream().anyMatch(taskId -> !taskManager.isTaskFinished(taskId))) {
            f.run();
        }
        for (int i = 0; i < 3; i++) {
            Thread.sleep( 2000);
            f.run();
        }
    }

}