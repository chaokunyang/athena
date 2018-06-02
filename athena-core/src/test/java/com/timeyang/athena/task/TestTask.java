package com.timeyang.athena.task;

import com.timeyang.athena.task.exec.Task;
import com.timeyang.athena.task.exec.TaskContext;

/**
 * @author yangck
 */
public class TestTask implements Task {

    @Override
    public void init(TaskContext ctx) {
        System.out.println("init");
    }

    @Override
    public void exec(TaskContext ctx) {
        for (int i = 0; i <= 10; i++) {
            try {
                Thread.sleep(500);
                System.out.println(ctx);
                System.out.format("exec: %d%% \n", i * 10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // throw new RuntimeException("error");
        }
    }

    @Override
    public void onSuccess(TaskContext ctx) {
        System.out.println("onSuccess");

    }

    @Override
    public void onError(TaskContext ctx, Throwable throwable) {
        System.out.println("onError");
    }

    @Override
    public void onLost(TaskContext ctx) {
        System.out.println("onLost");
    }
}
