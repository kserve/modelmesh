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

package com.ibm.watson.modelmesh;

import com.ibm.watson.litelinks.NettyCommon;
import com.ibm.watson.litelinks.server.DefaultThriftServer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
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
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This provides a pre-stop hook for colocated model runtime containers.
 * It blocks http requests until model-mesh is going to exit, to prevent
 * Kubernetes from shutting down those container prematurely (while we are
 * still propagating models to other instances).
 */
class RuntimeContainersPreStopServer implements Closeable {

    private static final Logger logger = LogManager.getLogger(RuntimeContainersPreStopServer.class);

    static final String PRESTOP_URI = "/prestop";

    private final int port;

    protected final Channel serverChannel;

    private final CountDownLatch latch = new CountDownLatch(1);
    private final AtomicInteger requestCount = new AtomicInteger();

    public RuntimeContainersPreStopServer(int port) throws Exception {
        this.port = port;
        this.serverChannel = startServer();
    }

    private Channel startServer() throws Exception {
        logger.info("Starting preStop http server on port " + port);

        EventLoopGroup elGroup = NettyCommon.newEventLoopGroup(1, "prestop-http-thread-%d");
        ServerBootstrap b = new ServerBootstrap()
                .group(elGroup, elGroup)
                .channel(NettyCommon.useEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new HttpRequestDecoder()).addLast(new HttpResponseEncoder());
                        p.addLast(new SimpleChannelInboundHandler<HttpRequest>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, HttpRequest req) {
                                boolean keepAlive = HttpUtil.isKeepAlive(req);
                                String uri = req.uri();
                                if (uri.endsWith("/")) {
                                    uri = uri.substring(0, uri.length() - 1);
                                }
                                HttpResponseStatus status = HttpResponseStatus.NOT_FOUND;

                                if (PRESTOP_URI.equals(uri)) {
                                    status = HttpResponseStatus.OK;

                                    Level logLevel = requestCount.incrementAndGet() < 10 ? Level.INFO : Level.WARN;
                                    logger.log(logLevel, "preStop api called for colocated container. "
                                                         + "Blocking until we are ready to exit, with timeout "
                                                         + (2 * DefaultThriftServer.SVC_IMPL_SHUTDOWN_TIMEOUT_SECS) + "s");
                                    try {
                                        latch.await(2 * DefaultThriftServer.SVC_IMPL_SHUTDOWN_TIMEOUT_SECS,
                                                TimeUnit.SECONDS);
                                    } catch (InterruptedException e) {
                                        logger.warn("preStop blocking interrupted!", e);
                                    }
                                }

                                HttpResponse resp = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status);
                                resp.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 0);
                                if (keepAlive) {
                                    resp.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
                                }
                                ChannelFuture cf = ctx.writeAndFlush(resp);
                                if (!keepAlive) {
                                    cf.addListener(ChannelFutureListener.CLOSE);
                                }
                            }

                            @Override
                            public void channelReadComplete(ChannelHandlerContext ctx) {
                                ctx.flush();
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ctx.close();
                                logger.error("preStop hook http server channel failed", cause);
                            }
                        });
                    }
                });

        return b.bind(port).channel();
    }

    @Override
    public void close() {
        logger.info("Stopping preStop http server on port " + port);
        latch.countDown();
        serverChannel.close();
    }

}
