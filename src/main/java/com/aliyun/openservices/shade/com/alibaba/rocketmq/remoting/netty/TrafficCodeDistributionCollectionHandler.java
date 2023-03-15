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

import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

@ChannelHandler.Sharable
public class TrafficCodeDistributionCollectionHandler extends ChannelDuplexHandler {

    private final ConcurrentMap<Integer, AtomicLong> inboundDistribution;
    private final ConcurrentMap<Integer, AtomicLong> outboundDistribution;

    public TrafficCodeDistributionCollectionHandler() {
        inboundDistribution = new ConcurrentHashMap<Integer, AtomicLong>();
        outboundDistribution = new ConcurrentHashMap<Integer, AtomicLong>();
    }

    private void countInbound(int responseCode) {
        AtomicLong item = inboundDistribution.get(responseCode);
        if (null == item) {
            item = new AtomicLong(0L);
            AtomicLong previous = inboundDistribution.putIfAbsent(responseCode, item);
            if (null != previous) {
                item = previous;
            }
        }
        item.incrementAndGet();
    }

    private void countOutbound(int responseCode) {
        AtomicLong item = outboundDistribution.get(responseCode);
        if (null == item) {
            item = new AtomicLong(0L);
            AtomicLong previous = outboundDistribution.putIfAbsent(responseCode, item);
            if (null != previous) {
                item = previous;
            }
        }
        item.incrementAndGet();
    }

    public Map<Integer, Long> inboundDistribution() {
        Map<Integer, Long> map = new HashMap<Integer, Long>(inboundDistribution.size());
        for (Map.Entry<Integer, AtomicLong> entry : inboundDistribution.entrySet()) {
            map.put(entry.getKey(), entry.getValue().getAndSet(0L));
        }
        return map;
    }

    public Map<Integer, Long> outboundDistribution() {
        Map<Integer, Long> map = new HashMap<Integer, Long>(outboundDistribution.size());
        for (Map.Entry<Integer, AtomicLong> entry : outboundDistribution.entrySet()) {
            map.put(entry.getKey(), entry.getValue().getAndSet(0L));
        }
        return map;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof RemotingCommand) {
            RemotingCommand cmd = (RemotingCommand) msg;
            countInbound(cmd.getCode());
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof RemotingCommand) {
            RemotingCommand cmd = (RemotingCommand) msg;
            countOutbound(cmd.getCode());
        }
        ctx.write(msg, promise);
    }
}
