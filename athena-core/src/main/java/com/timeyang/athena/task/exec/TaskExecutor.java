package com.timeyang.athena.task.exec;

import com.timeyang.athena.task.message.TaskMessage;
import com.timeyang.athena.task.message.TaskMessageCodec;
import com.timeyang.athena.utill.ParametersUtils;
import com.timeyang.athena.utill.StringUtils;
import com.timeyang.athena.utill.SystemUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.timeyang.athena.task.message.TaskMessage.*;

/**
 * TaskExecutor inspired by {@code org.apache.spark.executor.CoarseGrainedExecutorBackend}
 * <p>Download jars, set up URLClassLoader ...</p>
 * <ol>
 *     <li>For java task，loaded by {@link TaskExecutorLauncher}</li>
 *     <li>For spark task，submitted by spark-submit</li>
 *     <li>For mapreduce task, submitted by hadoop</li>
 *     <li>For flink task, submitted by flink</li>
 * </ol>
 * @author https://github.com/chaokunyang
 */
public class TaskExecutor {
    // static {
    //     PatternLayout layout = new PatternLayout("%d{yyyy-MM-dd HH:mm:ss} [%t] %-5p %c{2}:%L - %m%n");
    //     @SuppressWarnings("unchecked")
    //     ArrayList<Appender> list = Collections.list(org.apache.log4j.Logger.getRootLogger().getAllAppenders());
    //     list.forEach(appender -> appender.setLayout(layout));
    // }
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskExecutor.class);

    private final long taskId;
    private final String taskManagerHost;
    private final int taskManagerPort;

    private EventLoopGroup group;
    private Bootstrap b;
    private Channel channel;
    private Task task;
    private TaskContext taskContext;
    private LogInspection logInspection;
    // wait TaskManager send task object to TaskExecutor, and synchronizes memory
    private CountDownLatch latch = new CountDownLatch(1);

    public TaskExecutor(long taskId, String taskManagerHost, int taskManagerPort, String taskFilePath) {
        this.taskId = taskId;
        this.taskManagerHost = taskManagerHost;
        this.taskManagerPort = taskManagerPort;
        if (StringUtils.hasText(taskFilePath)) {
            this.logInspection = new LogInspection(taskFilePath);
        } else {
            LOGGER.warn("taskFilePath is null, taskExecutor won't provide log view feature");
        }

        Class<? extends Channel> channelClass;
        if(SystemUtils.isLinux()) {
            group = new EpollEventLoopGroup();
            channelClass = EpollSocketChannel.class;
        } else {
            group = new NioEventLoopGroup();
            channelClass = NioSocketChannel.class;
        }

        b = new Bootstrap();
        b.group(group)
                .channel(channelClass)
                .remoteAddress(new InetSocketAddress(taskManagerHost, taskManagerPort))
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch)
                            throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();

                        pipeline.addLast(new IdleStateHandler(0, 0, 60, TimeUnit.SECONDS));
                        pipeline.addLast(new TaskMessageCodec());
                        pipeline.addLast(new HeartbeatHandler());
                        pipeline.addLast(new TaskExecutorHandler());
                    }
                });
    }

    public void start() {
        try {
            channel = b.connect().sync().channel();
            LOGGER.info("TaskExecutor started");
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        if (channel != null) {
            channel.close().syncUninterruptibly();
        }
        group.shutdownGracefully();
        LOGGER.info("TaskExecutor stopped");
    }

    public void execute() {
        try {
            latch.await();
            try {
                task.exec(taskContext);
                LOGGER.info("task [{}] execute succeed", taskContext.taskId());
                try {
                    channel.writeAndFlush(new TaskSuccess(task));
                    LOGGER.info("Send TaskSuccess message succeed");
                } catch (Throwable e) {
                    LOGGER.info("Send TaskSuccess message failed");
                    e.printStackTrace();
                }
            } catch (Throwable throwable) {
                LOGGER.info("task [{}] execute failed", taskContext.taskId());
                throwable.printStackTrace();
                try {
                    channel.writeAndFlush(new TaskFailure(task, throwable));
                    LOGGER.info("Send TaskFailure message succeed");
                    channel.close().syncUninterruptibly();
                    System.exit(0);
                } catch (Throwable e) {
                    LOGGER.info("Send TaskFailure message failed");
                    e.printStackTrace();
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * exit when timeout
     */
    private class HeartbeatHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx,
                                       Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                ctx.writeAndFlush(new HeartBeat())
                        .addListener((ChannelFutureListener) future -> {
                            if (!future.isSuccess()) {
                                LOGGER.error("task [{}] connection to taskManager timeout.  Exit taskExecutor now", taskId, future.cause());
                                ctx.close().syncUninterruptibly();
                                System.exit(0);
                            }
                        });
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }
    }

    private class TaskExecutorHandler extends SimpleChannelInboundHandler<TaskMessage> {

        // handshake with TaskManager
        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            ByteBuf buffer = ctx.alloc().buffer();
            buffer.writeLong(taskId);
            buffer.writeInt(SystemUtils.getPID());
            ChannelFuture channelFuture = ctx.writeAndFlush(buffer);
            channelFuture.addListener(f -> {
                if (f.isSuccess()) {
                    LOGGER.info("task [{}] hand shake succeed", taskId);
                } else {
                    LOGGER.error("task [{}] hand shake failed. Exit taskExecutor now", taskId);
                    ctx.close().syncUninterruptibly();
                    System.exit(0);
                }
            });
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            LOGGER.error("Connection between taskExecutor and taskManager is disconnected. Exit taskExecutor now");
            System.exit(0);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, TaskMessage msg) throws Exception {
            if (msg instanceof TaskSubmit) {
                TaskSubmit submit = (TaskSubmit) msg;
                task = submit.getTask();
                taskContext = submit.getTaskContext();
                latch.countDown();
            }

            if (msg instanceof KillTask) {
                LOGGER.info("Received kill task command. Exit taskExecutor now");
                System.exit(0);
            }

            if (msg instanceof LogQueryRequest) {
                LogQueryRequest logQueryRequest = (LogQueryRequest) msg;
                LOGGER.info("log query request: " + logQueryRequest);
                List<String> lines;
                if (logInspection != null) {
                    lines = logInspection.getLines(
                            logQueryRequest.getLineNumber(), logQueryRequest.getRows());
                } else {
                    lines = new ArrayList<>();
                }
                LogQueryResult logQueryResult = new LogQueryResult(lines);
                ctx.writeAndFlush(logQueryResult);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            cause.printStackTrace();
            ctx.close();
            throw new RuntimeException(cause);
        }
    }

    public static void main(String[] args) {
        LOGGER.info("args: " + Arrays.asList(args));
        ParametersUtils parametersUtils = ParametersUtils.fromArgs(args);
        long taskId = parametersUtils.getLong("taskId");
        String taskManagerHost = parametersUtils.get("taskManagerHost");
        int taskManagerPort = parametersUtils.getInt("taskManagerPort");
        String taskFilePath = parametersUtils.get("taskFilePath");
        TaskExecutor executor = new TaskExecutor(taskId, taskManagerHost, taskManagerPort, taskFilePath);

        executor.start();
        executor.execute();
        executor.stop();
    }
}
