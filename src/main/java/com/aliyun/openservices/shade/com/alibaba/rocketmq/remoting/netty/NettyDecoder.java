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

import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLoggerFactory;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.common.RemotingUtil;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.protocol.RemotingCopyElisionRequestCode;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.nio.ByteBuffer;

public class NettyDecoder extends LengthFieldBasedFrameDecoder {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    private static final int FRAME_MAX_LENGTH =
        Integer.parseInt(System.getProperty("com.rocketmq.remoting.frameMaxLength", "16777216"));

    public NettyDecoder() {
        super(FRAME_MAX_LENGTH, 0, 4, 0, 4);
    }

    @Override
    public Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = null;
        try {
            frame = (ByteBuf) super.decode(ctx, in);
            if (null == frame) {
                return null;
            }

            ByteBuffer byteBuffer = frame.nioBuffer();
            RemotingCommand command = RemotingCommand.decode(byteBuffer);
            if (command.getBody() != null
                && RemotingCopyElisionRequestCode.COPY_ELISION_LIST.contains(command.getCode())) {
                command.retain(frame);
            }
            return command;
        } catch (Exception e) {
            log.error("decode exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);
            RemotingUtil.closeChannel(ctx.channel());
        } finally {
            if (null != frame) {
                frame.release();
                frame = null;
            }
        }

        return null;
    }
}
