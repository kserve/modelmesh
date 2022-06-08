/*
 * Copyright 2021 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.ibm.watson.prometheus;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.ibm.watson.litelinks.NettyCommon;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.ssl.NotSslRecordException;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;

/**
 * Netty-based prometheus endpoint
 */
public class NettyServer implements Closeable {
    private static final Logger logger = LogManager.getLogger(NettyServer.class);

    private static final boolean INFO_LOG_REQUESTS =
            "true".equalsIgnoreCase(System.getenv("MM_LOG_PROMETHEUS_REQS"));

    static final String PATH = "/metrics";

    private final int port;
    protected final Channel serverChannel;
    protected final CollectorRegistry registry;

    public NettyServer(CollectorRegistry registry, int port, boolean tls) throws Exception {
        this.registry = registry;
        this.port = port;
        this.serverChannel = startServer(tls);
    }

    private Channel startServer(boolean tls) throws Exception {
        logger.info("Starting prometheus " + (tls ? "HTTPS" : "HTTP") + " server on port " + port);
        final SslContext sslContext;
        if (tls) {
            // Note this requires bouncycastle library for java versions >= 15
            SelfSignedCertificate cert = new SelfSignedCertificate();
            sslContext = SslContextBuilder.forServer(cert.key(), cert.cert())
                .sslProvider(OpenSsl.isAvailable() ? SslProvider.OPENSSL : SslProvider.JDK).build();
        } else {
            sslContext = null;
        }

        EventLoopGroup elGroup = NettyCommon.newEventLoopGroup(1, "prom-http-thread-%d");
        ServerBootstrap b = new ServerBootstrap()
                .group(elGroup, elGroup)
                .channel(NettyCommon.useEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
//              .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    private int bufferSize = 512;

                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        if (sslContext != null) {
                            p.addLast(new SslHandler(sslContext.newEngine(ch.alloc())));
                        }
                        p.addLast(new HttpServerCodec()).addLast(new HttpContentCompressor());
                        p.addLast(new SimpleChannelInboundHandler<HttpRequest>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, HttpRequest req) {
                                QueryStringDecoder queryString = new QueryStringDecoder(req.uri());
                                String path = queryString.path();
                                if (path.endsWith("/")) path = path.substring(0, path.length()-1);
                                if (!PATH.equals(path) && !"/".equals(path)) {
                                    ctx.write(new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.NOT_FOUND));
                                    return;
                                }
                                boolean keepAlive = HttpUtil.isKeepAlive(req);
                                List<String> list = queryString.parameters().get("name[]");
                                Set<String> parsed = list == null || list.isEmpty()
                                        ? Collections.emptySet() : new HashSet<>(list);
                                ByteBuf buf = ctx.alloc().ioBuffer(bufferSize);
                                try {
                                    try (OutputStreamWriter osw = new OutputStreamWriter(new ByteBufOutputStream(buf))) {
                                        TextFormat.write004(osw, registry.filteredMetricFamilySamples(parsed));
                                    }
                                    HttpResponse resp = new DefaultFullHttpResponse(HTTP_1_1, OK, buf);
                                    int length = buf.readableBytes();
                                    resp.headers().setInt(CONTENT_LENGTH, length);
                                    if (keepAlive) resp.headers().set(CONNECTION, KEEP_ALIVE);
                                    if (bufferSize < length) bufferSize = length;
                                    ctx.write(resp);
                                    buf = null;
                                } catch (IOException ioe) {
                                    logger.warn("Error processing prometheus metrics HTTP request", ioe);
                                    ctx.write(new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.BAD_REQUEST));
                                } finally {
                                    if (buf != null) buf.release();
                                }
                                if (!keepAlive) {
                                    ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
                                }
                                if (INFO_LOG_REQUESTS) {
                                    logger.info("Served prometheus metrics HTTP(S) request");
                                } else {
                                    logger.debug("Served prometheus metrics HTTP(S) request");
                                }
                            }
                            @Override
                            public void channelReadComplete(ChannelHandlerContext ctx) {
                                ctx.flush();
                            }
                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ctx.close();
                                if (cause instanceof NotSslRecordException ||
                                        (cause.getCause() != null && cause.getCause() instanceof NotSslRecordException)) {
                                    logger.warn("Received non-HTTPS data on prometheus SSL-configured port " + port);
                                } else {
                                    logger.error("Prometheus HTTP(S) server processing failure", cause);
                                }
                            }
                        });
                    }
                });

        return b.bind(port).channel();
    }

    @Override
    public void close() {
        logger.info("Stopping prometheus HTTP(S) server on port " + port);
        serverChannel.close();
    }
}
