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

package com.aliyun.openservices.shade.com.alibaba.rocketmq.common.metrics;

import io.prometheus.client.Histogram;

public class MetricsTimerWrapper {
    private static final long TIMER_EXPIRED_SPAN = 60 * 1000L;

    private final long startTime;

    private final Histogram.Timer timer;

    public MetricsTimerWrapper(Histogram.Timer timer) {
        this.startTime = System.currentTimeMillis();
        this.timer = timer;
    }

    public long getStartTime() {
        return startTime;
    }

    public Histogram.Timer getTimer() {
        return timer;
    }

    public boolean isExpired() {
        return System.currentTimeMillis() - startTime > TIMER_EXPIRED_SPAN;
    }
}
