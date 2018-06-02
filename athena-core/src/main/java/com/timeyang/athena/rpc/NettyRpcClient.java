package com.timeyang.athena.rpc;

import com.timeyang.athena.utill.SystemUtils;
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

import java.io.Serializable;

/**
 * @author https://github.com/chaokunyang
 */
public class NettyRpcClient {
    private final String host;
    private final int port;
    private final EventLoopGroup group;
    private final Class<? extends SocketChannel> channelClass;
    private final Bootstrap b;
    private Channel channel;
    private Serializable response;

    public NettyRpcClient(String host, int port) {
        this.host = host;
        this.port = port;

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
        try {
            channel = b.connect(host, port).sync().channel();
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        if (channel != null) {
            channel.close();
        }
    }

    public synchronized Serializable request(Serializable msg) {
        channel.writeAndFlush(msg);
        try {
            while (response == null) wait();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return response;
    }

    private class DataExchangeHandler extends SimpleChannelInboundHandler<Serializable> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Serializable msg) throws Exception {
            synchronized (NettyRpcClient.this) {
                response = msg;
                notify();
            }
        }
    }
}

