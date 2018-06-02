package com.timeyang.athena.message;

import com.timeyang.athena.AthenaException;
import com.timeyang.athena.utill.SystemUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import java.security.cert.CertificateException;

/**
 * Message push service based on WebSocket
 *
 * @author https://github.com/chaokunyang
 */
public class MessageServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageServer.class);

    private String host;
    private int port;
    // all connected clients
    private final ChannelGroup channelGroup =
            new DefaultChannelGroup(ImmediateEventExecutor.INSTANCE);
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final Class<? extends ServerChannel> channelClass;
    private final ServerBootstrap bootstrap;
    private SslContext sslCtx;
    private Channel channel;

    public MessageServer(String host, int port, boolean enableSsl) {
        this.host = host;
        this.port = port;

        if (SystemUtils.IS_LINUX) {
            bossGroup = new EpollEventLoopGroup(1);
            workerGroup = new EpollEventLoopGroup();
            channelClass = EpollServerSocketChannel.class;
        } else {
            bossGroup = new NioEventLoopGroup(1);
            workerGroup = new NioEventLoopGroup();
            channelClass = NioServerSocketChannel.class;
        }

        if (enableSsl) {
            try {
                SelfSignedCertificate cert = new SelfSignedCertificate();
                LOGGER.warn("Use a temporary self-signed certificate, only for testing purposes, very insecure");
                sslCtx = SslContextBuilder
                        .forServer(cert.certificate(), cert.privateKey())
                        .build();
            } catch (SSLException e) {
                e.printStackTrace();
                throw new AthenaException("Can't create ssl context", e);
            } catch (CertificateException e) {
                e.printStackTrace();
                throw new AthenaException("Certificate problems", e);
            }
        }

        bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(channelClass)
                .childHandler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();

                        if(sslCtx != null) {
                            SSLEngine sslEngine = sslCtx.newEngine(ch.alloc());
                            sslEngine.setUseClientMode(false);
                            pipeline.addLast(new SslHandler(sslEngine));
                        }

                        pipeline.addLast(new HttpServerCodec());
                        pipeline.addLast(new HttpObjectAggregator(64 * 1024));
                        pipeline.addLast(new WebSocketServerProtocolHandler("/message"));
                        pipeline.addLast(new TextWebSocketFrameHandler(channelGroup));
                    }
                });
    }

    public void start() {
        ChannelFuture future = bootstrap.bind(host, port);
        future.syncUninterruptibly();
        channel = future.channel();
        LOGGER.info("MessageServer started");
    }

    public void stop() {
        if (channel != null) {
            channel.close();
        }
        channelGroup.close();
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
    }

    public void push(String message) {
        if (!channelGroup.isEmpty()) {
            workerGroup.execute(
                    () -> channelGroup.writeAndFlush(new TextWebSocketFrame(message)));
        }
    }
}
