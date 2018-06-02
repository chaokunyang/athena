package com.timeyang.athena.message;

import com.timeyang.athena.AthenaException;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketClientCompressionHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import javax.net.ssl.SSLException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author https://github.com/chaokunyang
 */
public final class WebSocketClient {
    private final String host;
    private final int port;
    private final URI uri;
    private SslContext sslCtx;
    private final EventLoopGroup group;
    private final Class<? extends SocketChannel> channelClass;
    private final Bootstrap b;
    private final WebSocketClientHandler handler;
    private Channel channel;

    public WebSocketClient(String url) {
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            throw new AthenaException("Url syntax error", e);
        }

        this.host = uri.getHost();
        String scheme = uri.getScheme() == null ? "ws" : uri.getScheme();
        if (!"ws".equalsIgnoreCase(scheme) && !"wss".equalsIgnoreCase(scheme)) {
            throw new AthenaException("Only WS(S) is supported");
        }
        if (uri.getPort() == -1) {
            if ("ws".equalsIgnoreCase(scheme)) {
                port = 80;
            } else if ("wss".equalsIgnoreCase(scheme)) {
                port = 443;
            } else {
                port = -1;
            }
        } else {
            port = uri.getPort();
        }

        final boolean ssl = "wss".equalsIgnoreCase(scheme);
        if (ssl) {
            try {
                sslCtx = SslContextBuilder
                        .forClient()
                        .trustManager(InsecureTrustManagerFactory.INSTANCE)
                        .build();
            } catch (SSLException e) {
                throw new AthenaException("Can't create ssl context", e);
            }
        }

        if (System.getProperty("os.name").toUpperCase().startsWith("LINUX")) {
            group = new EpollEventLoopGroup();
            channelClass = EpollSocketChannel.class;
        } else {
            group = new NioEventLoopGroup();
            channelClass = NioSocketChannel.class;
        }

        // Connect with V13 (RFC 6455 aka HyBi-17). You can change it to V08 or V00.
        // If you change it to V00, ping is not supported and remember to change
        // HttpResponseDecoder to WebSocketHttpResponseDecoder in the pipeline.
        handler = new WebSocketClientHandler(
                        WebSocketClientHandshakerFactory.newHandshaker(
                                uri, WebSocketVersion.V13, null,
                                true, new DefaultHttpHeaders()));

        b = new Bootstrap();
        b.group(group)
                .channel(channelClass)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        ChannelPipeline p = ch.pipeline();

                        if (sslCtx != null) {
                            p.addLast(sslCtx.newHandler(ch.alloc(), host, port));
                        }

                        p.addLast(
                                new HttpClientCodec(),
                                new HttpObjectAggregator(8192),
                                WebSocketClientCompressionHandler.INSTANCE,
                                handler);
                    }
                });

    }

    public void start() {
        try {
            channel = b.connect(uri.getHost(), port).sync().channel();
            handler.handshakeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        if (channel != null) {
            channel.close();
        }
        group.shutdownGracefully();
    }
}
