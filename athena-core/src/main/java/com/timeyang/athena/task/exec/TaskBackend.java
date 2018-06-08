package com.timeyang.athena.task.exec;

import com.timeyang.athena.AthenaConf;
import com.timeyang.athena.task.TaskInfo;
import com.timeyang.athena.task.message.TaskMessage;
import com.timeyang.athena.task.message.TaskMessage.LogQueryRequest;
import com.timeyang.athena.task.message.TaskMessage.LogQueryResult;
import com.timeyang.athena.task.message.TaskMessage.TaskFailure;
import com.timeyang.athena.task.message.TaskMessage.TaskSuccess;
import com.timeyang.athena.task.message.TaskMessageCodec;
import com.timeyang.athena.utill.ParametersUtils;
import com.timeyang.athena.utill.StringUtils;
import com.timeyang.athena.utill.SystemUtils;
import com.timeyang.athena.utill.cmd.CmdUtils;
import com.timeyang.athena.utill.cmd.Result;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * TaskBackend, responsible for communicating with TaskExecutor
 *
 * @author https://github.com/chaokunyang
 */
public class TaskBackend {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskBackend.class);

    private final String host;
    private final int port;
    private final AthenaConf athenaConf;

    private final TaskCallback taskCallback;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ServerBootstrap bootstrap;
    private Channel serverChannel;

    private final ChannelGroup channelGroup =
            new DefaultChannelGroup(ImmediateEventExecutor.INSTANCE);
    // remote task handles
    private final ConcurrentMap<Long, RemoteTaskHandle> remoteTasks = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, Task> taskInstances = new ConcurrentHashMap<>();
    private final Set<Long> startingTaskIds = ConcurrentHashMap.newKeySet();

    public TaskBackend(AthenaConf athenaConf, TaskCallback taskCallback) {
        this.host = athenaConf.getTaskRpcHost();
        this.port = athenaConf.getTaskRpcPort();
        this.athenaConf = athenaConf;
        this.taskCallback = taskCallback;

        Class<? extends ServerChannel> channelClass;
        if (SystemUtils.IS_LINUX) {
            bossGroup = new EpollEventLoopGroup(1);
            workerGroup = new EpollEventLoopGroup();
            channelClass = EpollServerSocketChannel.class;
        } else {
            bossGroup = new NioEventLoopGroup(1);
            workerGroup = new NioEventLoopGroup();
            channelClass = NioServerSocketChannel.class;
        }

        bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(channelClass)
                .childHandler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new IdleStateHandler(0, 0, athenaConf.getTaskHeartbeatTimeout(), TimeUnit.SECONDS));

                        pipeline.addLast(
                                new TaskHandShakeHandler());
                        pipeline.addLast(new TaskHeartbeatHandler());
                        pipeline.addLast(new TaskMessageCodec());
                        pipeline.addLast(new TaskHandler());
                    }
                });

    }

    public void start() {
        ChannelFuture future = bootstrap.bind(host, port);
        future.syncUninterruptibly();
        serverChannel = future.channel();
    }

    public void stop() {
        ChannelGroupFuture channelGroupFuture = channelGroup.writeAndFlush(new TaskMessage.KillTask());
        channelGroupFuture.syncUninterruptibly();

        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
        }
        channelGroup.close().syncUninterruptibly();
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
    }

    /**
     * @return true if task_cmd executed successfully
     */
    public boolean runTask(TaskInfo taskInfo) {
        Long taskId = taskInfo.getTaskId();
        LOGGER.debug("starting tasks: " + startingTaskIds);
        // start task if not started
        synchronized (startingTaskIds) {
            if (!startingTaskIds.contains(taskId)) {
                startingTaskIds.add(taskId);
            } else {
                LOGGER.debug("task [{}] is starting, won't run it repeatedly", taskId);
                return false;
            }
        }

        String className = taskInfo.getClassName();
        Task task;
        try {
            task = TaskUtils.createTask(className, ParametersUtils.fromArgs(taskInfo.getParams()).get());
        } catch (RuntimeException e) {
            LOGGER.error("Create task [{}] failed, move task to finished", taskId);
            taskCallback.onFailure(taskId);
            startingTaskIds.remove(taskId);
            taskInstances.remove(taskId);
            return false;
        }
        taskInstances.put(taskId, task);

        LOGGER.info("init task [{}]", taskId);
        task.init(TaskContextImpl.makeTaskContext(taskId));

        String taskCmd = TaskUtils.getTaskCmd(taskInfo, host, port);
        LOGGER.info("Starting task. task_start_cmd: [{}]", taskCmd);
        Result exec = CmdUtils.exec(taskInfo.getHost(), taskCmd);
        if (StringUtils.hasLength(exec.getOut()))
            LOGGER.info("task cmd output: " + exec.getOut());
        if (StringUtils.hasLength(exec.getError())) {
            LOGGER.warn("task error output: " + exec.getError());
        }

        if (exec.isSucceed()) {
            LOGGER.info("task_start_cmd of task [{}] executed, exit code {}", taskId, exec.getExitCode());
        } else {
            LOGGER.error("Execute task_cmd [{}] failed, exit code {}. Move task to finished", taskCmd, exec.getExitCode());

            taskCallback.onFailure(taskId);
            startingTaskIds.remove(taskId);
            taskInstances.remove(taskId);
        }

        return true;
    }

    public Future killTask(long taskId, Runnable runnable) {
        RemoteTaskHandle remoteTaskHandle = remoteTasks.get(taskId);
        Channel channel = remoteTaskHandle.getChannel();
        ChannelFuture channelFuture = channel.writeAndFlush(new TaskMessage.KillTask());
        channelFuture.addListener((ChannelFutureListener) future -> {
            removeTaskInfo(taskId);
            future.channel().close();
            runnable.run();
        });
        return channelFuture;
    }

    public boolean isTaskStarting(long taskId) {
        return startingTaskIds.contains(taskId);
    }

    public boolean isTaskRunning(long taskId) {
        return taskInstances.containsKey(taskId);
    }

    public List<String> getLogLines(long taskId, int lineNumber, int rows) {
        RemoteTaskHandle remoteTaskHandle = remoteTasks.get(taskId);
        if (remoteTaskHandle != null) {
            return remoteTaskHandle.getLogExchange().get(lineNumber, rows);
        }
        return new ArrayList<>();
    }

    /**
     * handshake with TaskExecutor
     * <ol>
     * <li>ad channel to ChannelGroup</li>
     * <li>taskId</li>
     * <li>remove TaskHandShakeHandler from pipeline when handshake finished</li>
     * </ol>
     * <ol>
     * massage：
     * <ol>
     * <li>taskId(8 bytes)</li>
     * <li>pid(4 bytes)</li>
     * </ol>
     */
    private class TaskHandShakeHandler extends ChannelInboundHandlerAdapter {
        private static final short TASK_ID_LENGTH = 8; // taskId: long
        private static final short PID_LENGTH = 4; // pid: int

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ByteBuf byteBuf = (ByteBuf) msg;

            if (byteBuf.readableBytes() < TASK_ID_LENGTH + PID_LENGTH)
                return;
            long taskId = byteBuf.readLong();
            int pid = byteBuf.readInt();

            channelGroup.add(ctx.channel());
            RemoteTaskHandle remoteTaskHandle =
                    new RemoteTaskHandle(taskId, pid, ctx.channel());
            remoteTasks.put(taskId, remoteTaskHandle);

            LOGGER.info("task [{}] hand shake finished, remove TaskHandShakeHandler from pipeline, fire TaskStarted event");
            ctx.pipeline().remove(this); // handshake finished, remove TaskHandShakeHandler from pipeline
            ctx.fireUserEventTriggered(new TaskStarted(remoteTaskHandle));
        }
    }

    /**
     * fire TaskMessage.TaskLost event when timeout
     */
    private static class TaskHeartbeatHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx,
                                       Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                ctx.writeAndFlush(new TaskMessage.HeartBeat())
                        .addListener((ChannelFutureListener) future -> {
                            if (!future.isSuccess()) {
                                ctx.fireUserEventTriggered(TaskEvent.TASK_LOST);
                            }
                        });
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }
    }

    private class TaskHandler extends SimpleChannelInboundHandler<TaskMessage> {
        private RemoteTaskHandle remoteTaskHandle;

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof TaskStarted) {
                this.remoteTaskHandle = ((TaskStarted) evt).getRemoteTaskHandle();

                long taskId = this.remoteTaskHandle.getTaskId();
                int pid = this.remoteTaskHandle.getPid();

                taskCallback.onStarted(taskId, pid);
                // task started. move task form waiting_task to running_task table
                // remove taskId from startingTaskIds
                startingTaskIds.remove(taskId);
                LOGGER.info("task [{}] {} started", taskId, taskInstances.get(taskId));

                Task task = taskInstances.get(taskId);
                TaskContext taskContext = TaskContextImpl.makeTaskContext(taskId);
                ctx.writeAndFlush(new TaskMessage.TaskSubmit(task, taskContext)); // send task to TaskExecutor
            } else if (evt == TaskEvent.TASK_LOST) {
                ctx.close(); // task lost, close channel
                long taskId = this.remoteTaskHandle.getTaskId();

                Task task = taskInstances.get(taskId);
                try {
                    task.onLost(TaskContextImpl.makeTaskContext(taskId));
                } catch (Throwable e) {
                    String msg = String.format("Call onLost method for task [%s] [%s]  failed", taskId, task);
                    LOGGER.warn(msg, e);
                }
                taskCallback.onLost(taskId);

                LOGGER.info("task [{}] {} lost connection", taskId, taskInstances.get(taskId));
                taskInstances.remove(taskId);
                remoteTasks.remove(taskId);
            }
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, TaskMessage msg) throws Exception {
            if (msg instanceof TaskSuccess) {
                long taskId = this.remoteTaskHandle.getTaskId();
                taskCallback.onSuccess(taskId);

                Task task = ((TaskSuccess) msg).getTask();
                try {
                    task.onSuccess(TaskContextImpl.makeTaskContext(taskId));
                } catch (Throwable throwable) {
                    String logMsg = String.format("Call task [%s] onSuccess method failed", task);
                    LOGGER.warn(logMsg, throwable);
                }

                LOGGER.info("task {} {} succeed", taskId, taskInstances.get(taskId));

                taskInstances.remove(taskId);
                remoteTasks.remove(taskId);
            } else if (msg instanceof TaskFailure) {
                long taskId = this.remoteTaskHandle.getTaskId();

                TaskFailure taskFailure = (TaskFailure) msg;
                Task task = taskFailure.getTask();
                Throwable throwable = taskFailure.getThrowable();
                String warnMsg = String.format("Task [%d] exec failed ", taskId);
                LOGGER.warn(warnMsg, throwable);
                try {
                    task.onError(TaskContextImpl.makeTaskContext(taskId), throwable);
                } catch (Throwable t) {
                    String logMsg = String.format("Call task [%s] onError method failed", task);
                    LOGGER.warn(logMsg, t);
                }

                LOGGER.info("task [{}] {} failed", taskId, taskInstances.get(taskId));
                taskCallback.onFailure(taskId);

                taskInstances.remove(taskId);
                remoteTasks.remove(taskId);
            } else if (msg instanceof LogQueryResult) {
                LogQueryResult logQueryResult = (LogQueryResult) msg;
                System.out.println("logQueryResult: " + logQueryResult);
                this.remoteTaskHandle.getLogExchange().set(logQueryResult.getLines());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            LOGGER.error(cause.getMessage(), cause);
            ctx.close();
        }
    }

    private void removeTaskInfo(long taskId) {
        taskInstances.remove(taskId);
        remoteTasks.remove(taskId);
        startingTaskIds.remove(taskId);
    }

    /**
     * TaskExecutor handler
     *
     * @author https://github.com/chaokunyang
     */
    private static class RemoteTaskHandle {
        private final long taskId;
        private final int pid;
        private final Channel channel;
        private final LogExchange logExchange;

        RemoteTaskHandle(long taskId, int pid, Channel channel) {
            this.taskId = taskId;
            this.pid = pid;
            this.channel = channel;
            this.logExchange = new LogExchange(channel);
        }

        public long getTaskId() {
            return taskId;
        }

        public int getPid() {
            return pid;
        }

        public Channel getChannel() {
            return channel;
        }

        public LogExchange getLogExchange() {
            return logExchange;
        }
    }

    /**
     *  single-threaded log query tool。
     * <p>由于只有一个channel，多线程会导致多条日志消息乱序。需要在TaskExecutor中启动一个Rpc服务，提供并发日志查询能力</p>\
     * <p>There is only one channel, multi-threaded will cause message out-of-order</p>
     * <p>If concurrent log query needed, provide log query rpc service on TaskExecutor</p>
     */
    private static class LogExchange {
        private final Channel channel;
        private List<String> lines;

        LogExchange(Channel channel) {
            this.channel = channel;
        }

        public synchronized void set(List<String> lines) {
            this.lines = lines;

            System.out.println("log lines: " + lines);
            notify();
        }

        public synchronized List<String> get(int lineNumber, int rows) {
            try {
                LogQueryRequest queryRequest = new LogQueryRequest(lineNumber, rows);
                channel.writeAndFlush(queryRequest).syncUninterruptibly();
                wait();
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
            }
            return lines;
        }
    }


    private enum TaskEvent {
        TASK_LOST
    }

    private static class TaskStarted {
        private final RemoteTaskHandle remoteTaskHandle;

        TaskStarted(RemoteTaskHandle remoteTaskHandle) {
            this.remoteTaskHandle = remoteTaskHandle;
        }

        public RemoteTaskHandle getRemoteTaskHandle() {
            return remoteTaskHandle;
        }

    }

}
