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

package com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.netty.protocol.http2proxy;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.netty.NettyServerConfig;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.netty.protocol.ProtocolHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;

public class Http2ProtocolProxyHandler implements ProtocolHandler {

    /**
     * The int value of "PRI ". Now use 4 bytes to judge protocol, may be has potential risks if there is a new protocol
     * which start with "PRI " too in the future
     * <p>
     * The full HTTP/2 connection preface is "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
     * <p>
     * ref: https://datatracker.ietf.org/doc/html/rfc7540#section-3.5
     */
    private static final int PRI_INT = 0x50524920;

    private final NettyServerConfig nettyServerConfig;

    public Http2ProtocolProxyHandler(NettyServerConfig nettyServerConfig) {
        this.nettyServerConfig = nettyServerConfig;
    }

    @Override
    public boolean match(ByteBuf in) {
        if (!nettyServerConfig.isEnableHttp2Proxy()) {
            return false;
        }

        // If starts with 'PRI '
        return in.getInt(in.readerIndex()) == PRI_INT;
    }

    @Override
    public void config(final ChannelHandlerContext ctx, final ByteBuf msg) {
        // proxy channel to http2 server
        final Channel inboundChannel = ctx.channel();

        // Start the connection attempt.
        Bootstrap b = new Bootstrap();
        b.group(inboundChannel.eventLoop())
            .channel(ctx.channel().getClass())
            .handler(new Http2ProxyBackendHandler(inboundChannel))
            .option(ChannelOption.AUTO_READ, false);
        ChannelFuture f = b.connect(nettyServerConfig.getHttp2ProxyHost(), nettyServerConfig.getHttp2ProxyPort());
        final Channel outboundChannel = f.channel();

        ctx.pipeline().addLast(new Http2ProxyFrontendHandler(outboundChannel));

        msg.retain();
        f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    outboundChannel.writeAndFlush(msg);
                    inboundChannel.read();
                } else {
                    inboundChannel.close();
                }
            }
        });
    }
}
