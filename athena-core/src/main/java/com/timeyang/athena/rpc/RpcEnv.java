package com.timeyang.athena.rpc;

import com.timeyang.athena.util.SystemUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.InetSocketAddress;

import static com.timeyang.athena.rpc.Message.*;

/**
 * @author https://github.com/chaokunyang
 */
public class RpcEnv {
    private final Logger LOGGER = LoggerFactory.getLogger(getClass());

    private final String host;
    private final int port;
    private final RpcEndpoint endpoint;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final Class<? extends ServerChannel> channelClass;
    private final ServerBootstrap bootstrap;
    private Channel channel;
    private InetSocketAddress addr;

    public RpcEnv(String host, int port, RpcEndpoint endpoint) {
        this.host = host;
        this.port = port;
        this.endpoint = endpoint;

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
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new ObjectDecoder(ClassResolvers.cacheDisabled(null)));
                        p.addLast(new ObjectEncoder());
                        p.addLast(new RpcHandler());
                    }
                });
    }

    public void start() {
        ChannelFuture future = bootstrap.bind(host, port);
        future.syncUninterruptibly();
        channel = future.channel();
        addr = (InetSocketAddress) channel.localAddress();
        LOGGER.info(this.getClass().getCanonicalName() + " started");
    }

    public void startAndWait() {
        start();
        channel.closeFuture().awaitUninterruptibly();
    }

    public void stop() {
        if (channel != null) {
            channel.close();
        }
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
    }

    public RpcAddress getAddr() {
        return new RpcAddress(addr.getHostString(), addr.getPort());
    }

    private class RpcHandler extends SimpleChannelInboundHandler<Message> {

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            endpoint.onConnected(getAddress(ctx));
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
            try {
                if (msg instanceof OnStart) {
                    endpoint.onStart();
                }
                if (msg instanceof OnStop) {
                    endpoint.onStop();
                }
                if (msg instanceof OneWayOutboxMessage) {
                    Object m = ((OneWayOutboxMessage) msg).getMsg();
                    endpoint.receive(m);
                }
                if (msg instanceof RpcOutboxMessage) {
                    Object m = ((RpcOutboxMessage) msg).getMsg();
                    Serializable serializable = endpoint.receiveAndReply(m);
                    Message message = new OneWayOutboxMessage(serializable);
                    ctx.writeAndFlush(message);
                }
            } catch (Throwable throwable) {
                endpoint.onError(throwable, getAddress(ctx));
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            endpoint.onDisconnected(getAddress(ctx));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            endpoint.onNetworkError(cause, getAddress(ctx));
        }

        private RpcAddress getAddress(ChannelHandlerContext ctx) {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) ctx.channel().remoteAddress();
            return new RpcAddress(inetSocketAddress.getHostString(), inetSocketAddress.getPort());
        }
    }

}

