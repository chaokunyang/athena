package com.timeyang.athena.rpc;

import com.timeyang.athena.util.SystemUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author https://github.com/chaokunyang
 */
public class NettyRpcClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(NettyRpcClient.class);
    private final String host;
    private final int port;
    private final long timeout;
    private final Bootstrap b;
    private Channel channel;
    private Serializable response;
    private final Lock lock = new ReentrantLock();
    private final Condition empty = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();

    /**
     * @param host host
     * @param port port
     * @param timeout timeout milliseconds
     */
    public NettyRpcClient(String host, int port, long timeout) {
        this.host = host;
        this.port = port;
        this.timeout = timeout;

        EventLoopGroup group;
        Class<? extends SocketChannel> channelClass;
        if (SystemUtils.IS_LINUX) {
            group = new EpollEventLoopGroup();
            channelClass = EpollSocketChannel.class;
        } else {
            group = new NioEventLoopGroup();
            channelClass = NioSocketChannel.class;
        }

        b = new Bootstrap();
        b.group(group)
                .channel(channelClass)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new ObjectDecoder(ClassResolvers.cacheDisabled(null)));
                        p.addLast(new ObjectEncoder());
                        p.addLast(new DataExchangeHandler());
                    }
                });
    }

    public void start() {
        channel = b.connect(host, port).syncUninterruptibly().channel();
        LOGGER.debug("NettyRpcClient started");
    }

    public void stop() {
        if (channel != null) {
            channel.close().syncUninterruptibly();
        }
        LOGGER.debug("NettyRpcClient stopped");
    }

    public Serializable request(Serializable msg) {
        try {
            lock.lock();
            channel.writeAndFlush(msg);
            try {
                while (response == null) {
                    if (!notEmpty.await(timeout, TimeUnit.MILLISECONDS)) {
                        throw new RpcTimeoutException(String.format("netty rpc client timeout for %d milliseconds", timeout));
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Serializable r = response;
            response = null;
            empty.signal();
            return r;
        } finally {
            lock.unlock();
        }
    }

    private class DataExchangeHandler extends SimpleChannelInboundHandler<Serializable> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Serializable msg) throws Exception {
            try {
                lock.lock();
                while (response != null) empty.await();
                response = msg;
                notEmpty.signal();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            LOGGER.error(cause.getMessage(), cause);
            ctx.close();
        }

    }
}




