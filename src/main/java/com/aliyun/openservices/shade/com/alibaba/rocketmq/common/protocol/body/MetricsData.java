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

package com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.body;

import com.alibaba.ons.open.trace.core.utils.JsonUtils;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.MixAll;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.UtilAll;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;

import java.io.IOException;

public class MetricsData extends RemotingSerializable {
    private String metrics;

    public String getMetrics() {
        return metrics;
    }

    public void setMetrics(String metrics) {
        this.metrics = metrics;
    }

    public byte[] encode(boolean compressed) throws IOException {
        if (!compressed) {
            return super.encode();
        }
        return UtilAll.compress(metrics.getBytes(MixAll.DEFAULT_CHARSET), 5);
    }

    public static MetricsData decode(byte[] data, boolean compressed) throws IOException {
        if (!compressed) {
            return JsonUtils.decode(data, MetricsData.class);
        }
        byte[] bytes = UtilAll.uncompress(data);
        MetricsData metricsData = new MetricsData();
        metricsData.setMetrics(new String(bytes, MixAll.DEFAULT_CHARSET));
        return metricsData;
    }
}
