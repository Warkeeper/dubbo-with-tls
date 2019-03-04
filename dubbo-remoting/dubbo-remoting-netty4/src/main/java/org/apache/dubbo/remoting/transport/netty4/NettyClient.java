/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.remoting.transport.netty4;

import io.netty.channel.*;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.transport.AbstractClient;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.File;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * NettyClient.
 */
public class NettyClient extends AbstractClient {

    private static final Logger logger = LoggerFactory.getLogger(NettyClient.class);

    private static final NioEventLoopGroup nioEventLoopGroup = new NioEventLoopGroup(Constants.DEFAULT_IO_THREADS,
            new DefaultThreadFactory("NettyClientWorker", true));

    private Bootstrap bootstrap;

    private volatile Channel channel; // volatile, please copy reference to use

    private boolean needTls = false;

    private boolean clientAuth = false;

    private File clientCert;

    private File secretKey;

    private File rootCa;

    public NettyClient(final URL url, final ChannelHandler handler) throws RemotingException {
        super(url, wrapChannelHandler(url, handler));
    }

    private void initTls() throws RemotingException {
        URL url = getUrl();
        String rootCaUrl = url.getParameter(Constants.TLS_SERVER_ROOT_CA_KEY);
        if (StringUtils.isEmpty(rootCaUrl)) {
            logger.info("Consumer " + url.getServiceInterface() + " has no tls config while trying to open netty 4.");
            needTls = false;
        } else {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            try {
                rootCa = new File(classLoader.getResource(rootCaUrl).getFile());
            } catch (NullPointerException npe) {
                throw new RemotingException(url.toInetSocketAddress(), null, "Failed to connect " + getClass().getSimpleName()
                        + " on " + getLocalAddress()
                        + ", cause: tls context file [root ca] cannot find , please check if the file exists: "
                        + rootCaUrl);
            }
            String clientCertUrl = url.getParameter(Constants.TLS_CLIENT_CERT_KEY);
            String secretKeyUrl = url.getParameter(Constants.TLS_CLIENT_SECRET_KEY);

            if (StringUtils.isEmpty(clientCertUrl) || StringUtils.isEmpty(secretKeyUrl)) {
                logger.info("Consumer " + url.getServiceInterface()
                        + " has no tls cert or secret key ,may be unavailable to communicate a provider which need client auth.");
            } else {
                try {
                    clientCert = new File(classLoader.getResource(clientCertUrl).getFile());
                    secretKey = new File(classLoader.getResource(secretKeyUrl).getFile());
                } catch (NullPointerException npe) {
                    throw new RemotingException(url.toInetSocketAddress(), null,
                            "Failed to connect " + getClass().getSimpleName()
                                    + " on " + getLocalAddress()
                                    + ", cause: tls context file [cert or secret key] cannot find , please check if these files exist: "
                                    + clientCertUrl + ", " + secretKeyUrl);
                }
                clientAuth = true;
            }
            needTls = true;
        }
    }

    @Override
    protected void doOpen() throws Throwable {
        initTls();
        final NettyClientHandler nettyClientHandler = new NettyClientHandler(getUrl(), this);
        bootstrap = new Bootstrap();
        bootstrap.group(nioEventLoopGroup)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                //.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, getTimeout())
                .channel(NioSocketChannel.class);

        if (getConnectTimeout() < 3000) {
            bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000);
        } else {
            bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, getConnectTimeout());
        }

        bootstrap.handler(new ChannelInitializer() {

            @Override
            protected void initChannel(Channel ch) throws Exception {
                int heartbeatInterval = UrlUtils.getIdleTimeout(getUrl());
                NettyCodecAdapter adapter = new NettyCodecAdapter(getCodec(), getUrl(), NettyClient.this);
                ChannelPipeline channelPipeline = ch
                        .pipeline();//.addLast("logging",new LoggingHandler(LogLevel.INFO))//for debug
                logger.info("needTls is " + needTls);
                //SSL Support
                if (needTls) {
                    SslContextBuilder sslContextBuilder = SslContextBuilder.forClient()
                            .sslProvider(SslProvider.OPENSSL)
                            .protocols("TLSv1.2")
                            .trustManager(rootCa);
                    if (clientAuth) {
                        sslContextBuilder.keyManager(clientCert, secretKey);
                    }
                    SslContext sslContext = sslContextBuilder.build();
                    channelPipeline.addFirst(sslContext.newHandler(ch.alloc()));
                    logger.info("tls context built~~");
                }
                channelPipeline.addLast("decoder", adapter.getDecoder())
                        .addLast("encoder", adapter.getEncoder())
                        .addLast("client-idle-handler", new IdleStateHandler(heartbeatInterval, 0, 0, MILLISECONDS))
                        .addLast("handler", nettyClientHandler);
            }
        });
    }

    @Override
    protected void doConnect() throws Throwable {
        long start = System.currentTimeMillis();
        ChannelFuture future = bootstrap.connect(getConnectAddress());
        try {
            boolean ret = future.awaitUninterruptibly(getConnectTimeout(), MILLISECONDS);

            if (ret && future.isSuccess()) {
                Channel newChannel = future.channel();
                try {
                    // Close old channel
                    Channel oldChannel = NettyClient.this.channel; // copy reference
                    if (oldChannel != null) {
                        try {
                            if (logger.isInfoEnabled()) {
                                logger.info(
                                        "Close old netty channel " + oldChannel + " on create new netty channel " + newChannel);
                            }
                            oldChannel.close();
                        } finally {
                            NettyChannel.removeChannelIfDisconnected(oldChannel);
                        }
                    }
                } finally {
                    if (NettyClient.this.isClosed()) {
                        try {
                            if (logger.isInfoEnabled()) {
                                logger.info("Close new netty channel " + newChannel + ", because the client closed.");
                            }
                            newChannel.close();
                        } finally {
                            NettyClient.this.channel = null;
                            NettyChannel.removeChannelIfDisconnected(newChannel);
                        }
                    } else {
                        NettyClient.this.channel = newChannel;
                    }
                }
            } else if (future.cause() != null) {
                throw new RemotingException(this, "client(url: " + getUrl() + ") failed to connect to server "
                        + getRemoteAddress() + ", error message is:" + future.cause().getMessage(), future.cause());
            } else {
                throw new RemotingException(this, "client(url: " + getUrl() + ") failed to connect to server "
                        + getRemoteAddress() + " client-side timeout "
                        + getConnectTimeout() + "ms (elapsed: " + (System.currentTimeMillis() - start)
                        + "ms) from netty client "
                        + NetUtils.getLocalHost() + " using dubbo version " + Version.getVersion());
            }
        } finally {
            if (!isConnected()) {
                //future.cancel(true);
            }
        }
    }

    @Override
    protected void doDisConnect() throws Throwable {
        try {
            NettyChannel.removeChannelIfDisconnected(channel);
        } catch (Throwable t) {
            logger.warn(t.getMessage());
        }
    }

    @Override
    protected void doClose() throws Throwable {
        //can't shutdown nioEventLoopGroup
        //nioEventLoopGroup.shutdownGracefully();
    }

    @Override
    protected org.apache.dubbo.remoting.Channel getChannel() {
        Channel c = channel;
        if (c == null || !c.isActive()) {
            return null;
        }
        return NettyChannel.getOrAddChannel(c, getUrl(), this);
    }

    @Override
    public boolean canHandleIdle() {
        return true;
    }
}
