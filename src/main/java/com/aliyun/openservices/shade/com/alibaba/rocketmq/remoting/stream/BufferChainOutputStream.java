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

import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLoggerFactory;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.google.common.base.Stopwatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

public class BufferChainOutputStream extends OutputStream {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    private final LinkedList<ByteBuf> bufferChain;
    private final ByteBufAllocator allocator;

    public BufferChainOutputStream(LinkedList<ByteBuf> bufferChain, ByteBufAllocator allocator) {
        this.bufferChain = bufferChain;
        this.allocator = allocator;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (bufferChain.isEmpty()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            ByteBuf slice = allocator.directBuffer(DefaultStream.INITIAL_SLICE_SIZE, DefaultStream.MAX_SLICE_SIZE);
            long allocationOverhead = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
            if (allocationOverhead > 0) {
                LOGGER.debug("Allocator spent {}ms allocating a buffer", allocationOverhead);
            }
            slice.writeBytes(b, off, len);
            bufferChain.addLast(slice);
        } else {
            ByteBuf slice = bufferChain.getLast();
            if (slice.writerIndex() + len < DefaultStream.MAX_SLICE_SIZE) {
                slice.writeBytes(b, off, len);
            } else {
                Stopwatch stopwatch = Stopwatch.createStarted();
                slice = allocator.directBuffer(DefaultStream.INITIAL_SLICE_SIZE, DefaultStream.MAX_SLICE_SIZE);
                long allocationOverhead = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
                if (allocationOverhead > 0) {
                    LOGGER.debug("Allocator spent {}ms allocating a buffer", allocationOverhead);
                }
                slice.writeBytes(b, off, len);
                bufferChain.addLast(slice);
            }
        }
    }

    @Override
    public void write(int b) throws IOException {
        throw new IOException("Unsupported");
    }
}
