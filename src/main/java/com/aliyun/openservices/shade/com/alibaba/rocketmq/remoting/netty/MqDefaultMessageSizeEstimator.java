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
import io.netty.channel.DefaultMessageSizeEstimator;
import io.netty.channel.MessageSizeEstimator;

/**
 * see  DefaultMessageSizeEstimator
 */
public class MqDefaultMessageSizeEstimator implements MessageSizeEstimator {

    private static final class HandleImpl implements Handle {
        private final int unknownSize;

        private HandleImpl(int unknownSize) {
            this.unknownSize = unknownSize;
        }

        @Override
        public int size(Object msg) {
            if (msg instanceof RemotingCommand) {
                RemotingCommand message = (RemotingCommand)msg;
                if (null != message.getBody()) {
                    return message.getBody().limit();
                }
                return unknownSize;
            }
            return DefaultMessageSizeEstimator.DEFAULT.newHandle().size(msg);
        }
    }

    @Override
    public Handle newHandle() {
        return handle;
    }

    /**
     * Return the default implementation which returns {@code 8} for unknown messages.
     */
    public static final MessageSizeEstimator DEFAULT = new MqDefaultMessageSizeEstimator(100);

    private final Handle handle;

    /**
     * Create a new instance
     *
     * @param unknownSize The size which is returned for unknown messages.
     */
    public MqDefaultMessageSizeEstimator(int unknownSize) {
        if (unknownSize < 0) {
            throw new IllegalArgumentException("unknownSize: " + unknownSize + " (expected: >= 0)");
        }
        handle = new MqDefaultMessageSizeEstimator.HandleImpl(unknownSize);
    }
}
