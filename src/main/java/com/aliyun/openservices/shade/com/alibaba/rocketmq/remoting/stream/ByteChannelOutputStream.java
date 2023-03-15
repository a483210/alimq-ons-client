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
package com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.stream;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLoggerFactory;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.common.RemotingUtil;

public class ByteChannelOutputStream extends OutputStream {

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    private int position;

    private final WritableByteChannel channel;

    public ByteChannelOutputStream(WritableByteChannel channel) {
        this.channel = channel;
        this.position = 0;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.wrap(b, off, len);
        int bytes = 0;
        while (byteBuffer.hasRemaining()) {
            bytes += channel.write(byteBuffer);
        }
        // Unless explicitly noted, channel should have written all of the provided byte sequence.
        assert len == bytes;
        position += bytes;
        // Log for debug purpose only
        String remote = "wire";
        if (channel instanceof SocketChannel) {
            remote = RemotingUtil.socketAddress2String(((SocketChannel) channel).socket().getRemoteSocketAddress());
        }
        LOGGER.debug("Data written to {} in stream API: {}", remote, new String(b, off, len));
    }

    @Override
    public void write(int b) throws IOException {
        throw new IOException("Unsupported");
    }

    public int getPosition() {
        return position;
    }
}
