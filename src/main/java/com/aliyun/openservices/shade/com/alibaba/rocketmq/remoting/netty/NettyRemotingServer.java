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
package com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.netty;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.ProcessHandle;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLoggerFactory;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.ChannelEventListener;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.InvokeCallback;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.RPCHook;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.RemotingServer;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.common.Pair;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.common.RemotingUtil;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.common.TlsMode;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.netty.protocol.ProtocolNegotiationHandler;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.netty.protocol.http2proxy.Http2ProtocolProxyHandler;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.netty.protocol.remoting.RemotingProtocolHandler;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.protocol.RemotingCommandType;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.vtoa.VpcTunnelUtils;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.vtoa.Vtoa;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.util.*;
import java.util.concurrent.*;

public class NettyRemotingServer extends NettyRemotingAbstract implements RemotingServer {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);
    private static final InternalLogger TRAFFIC_LOGGER =
        InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_TRAFFIC);
    private final ServerBootstrap serverBootstrap;
    private final EventLoopGroup eventLoopGroupSelector;
    private final EventLoopGroup eventLoopGroupBoss;
    private final NettyServerConfig nettyServerConfig;

    private final ExecutorService publicExecutor;
    private final ChannelEventListener channelEventListener;

    private final Timer timer = new Timer("ServerHouseKeepingService", true);
    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    private final List<RPCHook> rpcHookList = new CopyOnWriteArrayList<RPCHook>();
    private final ConcurrentHashMap<Channel, Vtoa> tunnelTable = new ConcurrentHashMap<Channel, Vtoa>(16);

    private int port = 0;

    private static final String HANDSHAKE_HANDLER_NAME = "handshakeHandler";
    private static final String TLS_HANDLER_NAME = "sslHandler";
    private static final String FILE_REGION_ENCODER_NAME = "fileRegionEncoder";

    private final ScheduledExecutorService scheduledExecutorService;
    private final TrafficCodeDistributionCollectionHandler trafficCodeDistributionCollectionHandler;

    private int pid;

    public NettyRemotingServer(final NettyServerConfig nettyServerConfig) {
        this(nettyServerConfig, null);
    }

    public NettyRemotingServer(final NettyServerConfig nettyServerConfig,
        final ChannelEventListener channelEventListener) {
        super(nettyServerConfig.getServerOnewaySemaphoreValue(), nettyServerConfig.getServerAsyncSemaphoreValue());
        this.serverBootstrap = new ServerBootstrap();
        this.nettyServerConfig = nettyServerConfig;
        this.channelEventListener = channelEventListener;

        int publicThreadNums = nettyServerConfig.getServerCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }

        this.publicExecutor = new ThreadPoolExecutor(publicThreadNums, publicThreadNums, 1, TimeUnit.MINUTES,
            new LinkedBlockingDeque<Runnable>(), new ThreadFactoryImpl("NettyServerPublicExecutor_"),
            new ThreadPoolExecutor.CallerRunsPolicy());

        this.eventLoopGroupBoss = new NioEventLoopGroup(1, new ThreadFactoryImpl("NettyBoss_"));

        if (useEpoll()) {
            this.eventLoopGroupSelector = new EpollEventLoopGroup(nettyServerConfig.getServerSelectorThreads(),
                new ThreadFactoryImpl("NettyServerEPOLLSelector_"));
        } else {
            this.eventLoopGroupSelector = new NioEventLoopGroup(nettyServerConfig.getServerSelectorThreads(),
                new ThreadFactoryImpl("NettyServerNIOSelector_"));
        }

        loadSslContext();

        scheduledExecutorService = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("NettyServerScheduler_"),
            new ThreadPoolExecutor.DiscardOldestPolicy());
        trafficCodeDistributionCollectionHandler = new TrafficCodeDistributionCollectionHandler();
        pid = -1;
    }

    public void loadSslContext() {
        TlsMode tlsMode = TlsSystemConfig.tlsMode;
        log.info("Server is running in TLS {} mode", tlsMode.getName());

        if (tlsMode != TlsMode.DISABLED) {
            try {
                sslContext = TlsHelper.buildSslContext(false);
                log.info("SSLContext created for server");
            } catch (CertificateException e) {
                log.error("Failed to create SSLContext for server", e);
            } catch (IOException e) {
                log.error("Failed to create SSLContext for server", e);
            }
        }
    }

    private boolean useEpoll() {
        return RemotingUtil.isLinuxPlatform()
            && nettyServerConfig.isUseEpollNativeSelector()
            && Epoll.isAvailable();
    }

    /**
     * Log request/response code distribution
     */
    private void logTrafficCodeDistribution() {
        logDistribution("RequestCode Distribution", trafficCodeDistributionCollectionHandler.inboundDistribution());
        logDistribution("ResponseCode Distribution", trafficCodeDistributionCollectionHandler.outboundDistribution());
    }

    private void logDistribution(String title, Map<Integer, Long> distribution) {
        if (null != distribution && !distribution.isEmpty()) {
            StringBuilder sb = new StringBuilder(title);
            sb.append(": [");
            boolean first = true;
            for (Map.Entry<Integer, Long> entry : distribution.entrySet()) {
                if (0L == entry.getValue()) {
                    continue;
                }
                if (!first) {
                    sb.append(", ");
                }
                sb.append(entry.getKey()).append(":").append(entry.getValue());
                first = false;
            }
            if (first) {
                return;
            }
            sb.append("]");
            if (pid < 0) {
                pid = ProcessHandle.pid();
            }
            TRAFFIC_LOGGER.info("ProcessID={}, {}", pid, sb.toString());
        }
    }

    @Override
    public void start() {
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
            nettyServerConfig.getServerWorkerThreads(),
            new ThreadFactoryImpl("NettyServerCodecThread_"));

        WriteBufferWaterMark writeBufferWaterMark =
            new WriteBufferWaterMark(nettyServerConfig.getWriteBufferLowWaterMark(),
                nettyServerConfig.getWriteBufferHighWaterMark());
        ServerBootstrap childHandler =
            this.serverBootstrap.group(this.eventLoopGroupBoss, this.eventLoopGroupSelector)
                .channel(useEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.SO_KEEPALIVE, false)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_SNDBUF, nettyServerConfig.getServerSocketSndBufSize())
                .childOption(ChannelOption.SO_RCVBUF, nettyServerConfig.getServerSocketRcvBufSize())
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, writeBufferWaterMark)
                .childOption(ChannelOption.MESSAGE_SIZE_ESTIMATOR, MqDefaultMessageSizeEstimator.DEFAULT)
                .localAddress(new InetSocketAddress(this.nettyServerConfig.getListenPort()))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline channelPipelineWithTls = ch.pipeline()
                            .addLast(defaultEventExecutorGroup, HANDSHAKE_HANDLER_NAME,
                                new HandshakeHandler(TlsSystemConfig.tlsMode));
                        if (nettyServerConfig.isEnableProtocolNegotiation()) {
                            channelPipelineWithTls.addLast(defaultEventExecutorGroup,
                                new IdleStateHandler(0, 0, nettyServerConfig.getServerChannelMaxIdleTimeSeconds()),
                                new ProtocolNegotiationHandler(new RemotingProtocolHandler(NettyRemotingServer.this))
                                    .addProtocolHandler(new Http2ProtocolProxyHandler(nettyServerConfig))
                            );
                        } else {
                            channelPipelineWithTls.addLast(defaultEventExecutorGroup,
                                new NettyEncoder(),
                                new NettyDecoder(),
                                trafficCodeDistributionCollectionHandler,
                                new IdleStateHandler(0, 0, nettyServerConfig.getServerChannelMaxIdleTimeSeconds()),
                                new NettyConnectManageHandler(),
                                new NettyServerHandler()
                            );
                        }
                    }
                });

        if (nettyServerConfig.isServerPooledByteBufAllocatorEnable()) {
            childHandler.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }

        try {
            ChannelFuture sync = this.serverBootstrap.bind().sync();
            InetSocketAddress addr = (InetSocketAddress) sync.channel().localAddress();
            this.port = addr.getPort();
        } catch (InterruptedException e1) {
            throw new RuntimeException("this.serverBootstrap.bind().sync() InterruptedException", e1);
        }

        if (this.channelEventListener != null) {
            this.nettyEventExecutor.start();
        }

        this.timer.scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {
                try {
                    NettyRemotingServer.this.scanResponseTable();
                } catch (Throwable e) {
                    log.error("scanResponseTable exception", e);
                }
            }
        }, 1000 * 3, 1000);

        scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    NettyRemotingServer.this.logTrafficCodeDistribution();
                } catch (Throwable e) {
                    log.error("Unexpected exception raised while log response code distribution", e);
                }
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public void shutdown() {
        try {
            this.timer.cancel();

            this.eventLoopGroupBoss.shutdownGracefully();

            this.eventLoopGroupSelector.shutdownGracefully();

            this.nettyEventExecutor.shutdown();

            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        } catch (Exception e) {
            log.error("NettyRemotingServer shutdown exception, ", e);
        }

        if (this.publicExecutor != null) {
            try {
                this.publicExecutor.shutdown();
            } catch (Exception e) {
                log.error("NettyRemotingServer shutdown exception, ", e);
            }
        }
    }

    @Override
    public void registerRPCHook(RPCHook rpcHook) {
        this.rpcHookList.add(rpcHook);
    }

    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
        ExecutorService executorThis = executor;
        if (null == executor) {
            executorThis = this.publicExecutor;
        }

        Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<NettyRequestProcessor, ExecutorService>(processor, executorThis);
        this.processorTable.put(requestCode, pair);
    }

    @Override
    public void registerDefaultProcessor(NettyRequestProcessor processor, ExecutorService executor) {
        this.defaultRequestProcessor = new Pair<NettyRequestProcessor, ExecutorService>(processor, executor);
    }

    @Override
    public int localListenPort() {
        return this.port;
    }

    @Override
    public Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(int requestCode) {
        return processorTable.get(requestCode);
    }

    @Override
    public Pair<NettyRequestProcessor, ExecutorService> getDefaultProcessorPair() {
        return defaultRequestProcessor;
    }

    @Override
    public RemotingCommand invokeSync(final Channel channel, final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        return this.invokeSyncImpl(channel, request, timeoutMillis);
    }

    @Override
    public void invokeAsync(Channel channel, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback)
        throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        this.invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
    }

    @Override
    public void invokeOneway(Channel channel, RemotingCommand request, long timeoutMillis) throws InterruptedException,
        RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        request.markOnewayRPC();
        this.invokeOnewayImpl(channel, request, timeoutMillis);
    }

    @Override
    public ChannelEventListener getChannelEventListener() {
        return channelEventListener;
    }

    @Override
    public List<RPCHook> getRPCHook() {
        return rpcHookList;
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return this.publicExecutor;
    }

    class HandshakeHandler extends SimpleChannelInboundHandler<ByteBuf> {

        private final TlsMode tlsMode;

        private static final byte HANDSHAKE_MAGIC_CODE = 0x16;

        HandshakeHandler(TlsMode tlsMode) {
            this.tlsMode = tlsMode;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {

            // mark the current position so that we can peek the first byte to determine if the content is starting with
            // TLS handshake
            msg.markReaderIndex();

            byte b = msg.getByte(0);

            if (b == HANDSHAKE_MAGIC_CODE) {
                switch (tlsMode) {
                    case DISABLED:
                        ctx.close();
                        log.warn("Clients intend to establish a SSL connection while this server is running in SSL disabled mode");
                        break;
                    case PERMISSIVE:
                    case ENFORCING:
                        if (null != sslContext) {
                            ctx.pipeline()
                                .addAfter(defaultEventExecutorGroup, HANDSHAKE_HANDLER_NAME, TLS_HANDLER_NAME, sslContext.newHandler(ctx.channel().alloc()))
                                .addAfter(defaultEventExecutorGroup, TLS_HANDLER_NAME, FILE_REGION_ENCODER_NAME, new FileRegionEncoder());
                            log.info("Handlers prepended to channel pipeline to establish SSL connection");
                        } else {
                            ctx.close();
                            log.error("Trying to establish a SSL connection but sslContext is null");
                        }
                        break;

                    default:
                        log.warn("Unknown TLS mode");
                        break;
                }
            } else if (tlsMode == TlsMode.ENFORCING) {
                ctx.close();
                log.warn("Clients intend to establish an insecure connection while this server is running in SSL enforcing mode");
            }

            // reset the reader index so that handshake negotiation may proceed as normal.
            msg.resetReaderIndex();

            try {
                // Remove this handler
                ctx.pipeline().remove(this);
            } catch (NoSuchElementException e) {
                log.error("Error while removing HandshakeHandler", e);
            }

            // Hand over this message to the next .
            ctx.fireChannelRead(msg.retain());
        }
    }

    class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processTunnelId(ctx, msg);
            processMessageReceived(ctx, msg);
        }

        public void processTunnelId(ChannelHandlerContext ctx, RemotingCommand msg) {
            if (nettyServerConfig.isAttachVtoaInfoEnable()) {
                if (null != msg && msg.getType() == RemotingCommandType.REQUEST_COMMAND) {
                    Vtoa vtoa = tunnelTable.get(ctx.channel());
                    if (null == vtoa) {
                        vtoa = VpcTunnelUtils.getInstance().getVtoa(ctx);
                        tunnelTable.put(ctx.channel(), vtoa);
                    }
                    msg.addExtField(VpcTunnelUtils.PROPERTY_VTOA_TUNNEL_ID, String.valueOf(vtoa.getVid()));
                    msg.addExtField(VpcTunnelUtils.PROPERTY_VTOA_CLIENT_ADDR, String.valueOf(vtoa.getVaddr()));
                    msg.addExtField(VpcTunnelUtils.PROPERTY_VTOA_CLIENT_PORT, String.valueOf(vtoa.getVport()));
                }
            }
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
            Channel channel = ctx.channel();
            if (channel.isWritable()) {
                if (!channel.config().isAutoRead()) {
                    channel.config().setAutoRead(true);
                    log.info("Channel[{}] turns writable, bytes to buffer before " +
                            "changing channel to un-writable: {}",
                        RemotingHelper.parseChannelRemoteAddr(channel),
                        channel.bytesBeforeUnwritable());
                }
            } else {
                channel.config().setAutoRead(false);
                log.warn("Auto-read of channel[{}] is disabled! Bytes to drain before it turns writable: {}",
                        RemotingHelper.parseChannelRemoteAddr(channel), channel.bytesBeforeWritable());
            }
            super.channelWritabilityChanged(ctx);
        }
    }

    class NettyConnectManageHandler extends ChannelDuplexHandler {
        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelRegistered {}", remoteAddress);
            if (nettyServerConfig.isAttachVtoaInfoEnable()) {
                tunnelTable.put(ctx.channel(), VpcTunnelUtils.getInstance().getVtoa(ctx));
            }
            super.channelRegistered(ctx);
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelUnregistered, the channel[{}]", remoteAddress);
            if (nettyServerConfig.isAttachVtoaInfoEnable()) {
                tunnelTable.remove(ctx.channel());
            }
            super.channelUnregistered(ctx);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelActive, the channel[{}]", remoteAddress);
            super.channelActive(ctx);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelInactive, the channel[{}]", remoteAddress);
            super.channelInactive(ctx);

            if (nettyServerConfig.isAttachVtoaInfoEnable()) {
                tunnelTable.remove(ctx.channel());
            }
            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    log.warn("NETTY SERVER PIPELINE: IDLE exception [{}]", remoteAddress);
                    RemotingUtil.closeChannel(ctx.channel());
                    if (nettyServerConfig.isAttachVtoaInfoEnable()) {
                        tunnelTable.remove(ctx.channel());
                    }
                    if (NettyRemotingServer.this.channelEventListener != null) {
                        NettyRemotingServer.this
                            .putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress, ctx.channel()));
                    }
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.warn("NETTY SERVER PIPELINE: exceptionCaught {}", remoteAddress);
            log.warn("NETTY SERVER PIPELINE: exceptionCaught exception.", cause);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress, ctx.channel()));
            }

            if (nettyServerConfig.isAttachVtoaInfoEnable()) {
                tunnelTable.remove(ctx.channel());
            }
            RemotingUtil.closeChannel(ctx.channel());
        }
    }

    public TrafficCodeDistributionCollectionHandler getTrafficCodeDistributionCollectionHandler() {
        return this.trafficCodeDistributionCollectionHandler;
    }

    public NettyServerHandler newNettyServerHandler() {
        return new NettyServerHandler();
    }

    public NettyConnectManageHandler newNettyConnectManageHandler() {
        return new NettyConnectManageHandler();
    }
}
