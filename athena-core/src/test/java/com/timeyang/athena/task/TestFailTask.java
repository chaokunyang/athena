package com.timeyang.athena.task;

import com.timeyang.athena.task.exec.Task;
import com.timeyang.athena.task.exec.TaskContext;

/**
 * @author yangck
 */
public class TestFailTask implements Task {

    @Override
    public void init(TaskContext ctx) {
        System.out.println(TestFailTask.class + " init");
    }

    @Override
    public void exec(TaskContext ctx) {
        for (int i = 0; i <= 10; i++) {
            try {
                Thread.sleep(500);
                System.out.println(ctx);
                System.out.format(TestFailTask.class + " exec: %d%% \n", i * 10);

                if (i == 2)
                    throw new RuntimeException("error");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void onSuccess(TaskContext ctx) {
        System.out.println(TestFailTask.class + " onSuccess");

    }

    @Override
    public void onError(TaskContext ctx, Throwable throwable) {
        System.out.println(TestFailTask.class + " onError");
    }

    @Override
    public void onLost(TaskContext ctx) {
        System.out.println(TestFailTask.class + " onLost");
    }
}
