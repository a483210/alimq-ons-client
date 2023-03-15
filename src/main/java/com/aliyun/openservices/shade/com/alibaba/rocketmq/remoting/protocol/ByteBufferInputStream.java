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
package com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.protocol;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends InputStream {
    private final ByteBuffer byteBuffer;

    private int markPosition;

    public ByteBufferInputStream(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (null == byteBuffer || !byteBuffer.hasRemaining()) {
            return -1;
        }
        int length = len;
        if (length > b.length - off) {
            length = b.length - off;
        }

        if (length > byteBuffer.remaining()) {
            length = byteBuffer.remaining();
        }
        byteBuffer.get(b, off, length);
        return length;
    }

    @Override
    public long skip(long n) throws IOException {
        if (null == byteBuffer || !byteBuffer.hasRemaining()) {
            return 0;
        }

        long skipped = n;
        if (skipped > byteBuffer.remaining()) {
            skipped = byteBuffer.remaining();
        }

        byteBuffer.position(byteBuffer.position() + (int) skipped);
        return skipped;
    }

    @Override
    public int available() throws IOException {
        return byteBuffer.remaining();
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public synchronized void mark(int readlimit) {
        if (null != byteBuffer) {
            markPosition = byteBuffer.position();
        }
    }

    @Override
    public synchronized void reset() throws IOException {
        if (null != byteBuffer) {
            byteBuffer.position(markPosition);
        }
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public int read() throws IOException {
        if (null == byteBuffer || !byteBuffer.hasRemaining()) {
            return -1;
        }
        return byteBuffer.get();
    }
}
