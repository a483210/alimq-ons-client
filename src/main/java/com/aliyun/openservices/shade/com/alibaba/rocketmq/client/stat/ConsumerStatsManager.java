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

package com.aliyun.openservices.shade.com.alibaba.rocketmq.client.stat;

import java.util.concurrent.ScheduledExecutorService;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.log.ClientLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.body.ConsumeStatus;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.stats.StatsItemSet;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.stats.StatsSnapshot;

public class ConsumerStatsManager {
    private static final InternalLogger log = ClientLogger.getLog();

    private static final String TOPIC_AND_GROUP_CONSUME_OK_TPS = "CONSUME_OK_TPS";
    private static final String TOPIC_AND_GROUP_CONSUME_FAILED_TPS = "CONSUME_FAILED_TPS";
    private static final String TOPIC_AND_GROUP_CONSUME_RT = "CONSUME_RT";
    private static final String TOPIC_AND_GROUP_PULL_TPS = "PULL_TPS";
    private static final String TOPIC_AND_GROUP_PULL_RT = "PULL_RT";

    private final StatsItemSet topicAndGroupConsumeOKTPS;
    private final StatsItemSet topicAndGroupConsumeRT;
    private final StatsItemSet topicAndGroupConsumeFailedTPS;
    private final StatsItemSet topicAndGroupPullTPS;
    private final StatsItemSet topicAndGroupPullRT;

    private static final String POP_RT = "POP_RT";
    private static final String POP_QPS = "POP_QPS";
    private static final String PEEK_RT = "PEEK_RT";
    private static final String PEEK_QPS = "PEEK_QPS";
    private static final String ACK_RT = "ACK_RT";
    private static final String ACK_QPS = "ACK_QPS";
    private static final String CHANGE_RT = "CHANGE_RT";
    private static final String CHANGE_QPS = "CHANGE_QPS";

    private final StatsItemSet popRT;
    private final StatsItemSet popQPS;
    private final StatsItemSet peekRT;
    private final StatsItemSet peekQPS;
    private final StatsItemSet ackRT;
    private final StatsItemSet ackQPS;
    private final StatsItemSet changeRT;
    private final StatsItemSet changeQPS;

    public ConsumerStatsManager(final ScheduledExecutorService scheduledExecutorService) {
        this.topicAndGroupConsumeOKTPS =
            new StatsItemSet(TOPIC_AND_GROUP_CONSUME_OK_TPS, scheduledExecutorService, log);

        this.topicAndGroupConsumeRT =
            new StatsItemSet(TOPIC_AND_GROUP_CONSUME_RT, scheduledExecutorService, log);

        this.topicAndGroupConsumeFailedTPS =
            new StatsItemSet(TOPIC_AND_GROUP_CONSUME_FAILED_TPS, scheduledExecutorService, log);

        this.topicAndGroupPullTPS = new StatsItemSet(TOPIC_AND_GROUP_PULL_TPS, scheduledExecutorService, log);

        this.topicAndGroupPullRT = new StatsItemSet(TOPIC_AND_GROUP_PULL_RT, scheduledExecutorService, log);

        this.popRT = new StatsItemSet(POP_RT, scheduledExecutorService, log);
        this.popQPS = new StatsItemSet(POP_QPS, scheduledExecutorService, log);
        this.peekRT = new StatsItemSet(PEEK_RT, scheduledExecutorService, log);
        this.peekQPS = new StatsItemSet(PEEK_QPS, scheduledExecutorService, log);
        this.ackRT = new StatsItemSet(ACK_RT, scheduledExecutorService, log);
        this.ackQPS = new StatsItemSet(ACK_QPS, scheduledExecutorService, log);
        this.changeRT = new StatsItemSet(CHANGE_RT, scheduledExecutorService, log);
        this.changeQPS = new StatsItemSet(CHANGE_QPS, scheduledExecutorService, log);
    }

    public void start() {
    }

    public void shutdown() {
    }

    public void incPullRT(final String group, final String topic, final long rt) {
        this.topicAndGroupPullRT.addValue(topic + "@" + group, (int) rt, 1);
    }

    public void incPullTPS(final String group, final String topic, final long msgs) {
        this.topicAndGroupPullTPS.addValue(topic + "@" + group, (int) msgs, 1);
    }

    public void incConsumeRT(final String group, final String topic, final long rt) {
        this.topicAndGroupConsumeRT.addValue(topic + "@" + group, (int) rt, 1);
    }

    public void incConsumeOKTPS(final String group, final String topic, final long msgs) {
        this.topicAndGroupConsumeOKTPS.addValue(topic + "@" + group, (int) msgs, 1);
    }

    public void incConsumeFailedTPS(final String group, final String topic, final long msgs) {
        this.topicAndGroupConsumeFailedTPS.addValue(topic + "@" + group, (int) msgs, 1);
    }

    public ConsumeStatus consumeStatus(final String group, final String topic) {
        ConsumeStatus cs = new ConsumeStatus();
        {
            StatsSnapshot ss = this.getPullRT(group, topic);
            if (ss != null) {
                cs.setPullRT(ss.getAvgpt());
            }
        }

        {
            StatsSnapshot ss = this.getPullTPS(group, topic);
            if (ss != null) {
                cs.setPullTPS(ss.getTps());
            }
        }

        {
            StatsSnapshot ss = this.getConsumeRT(group, topic);
            if (ss != null) {
                cs.setConsumeRT(ss.getAvgpt());
            }
        }

        {
            StatsSnapshot ss = this.getConsumeOKTPS(group, topic);
            if (ss != null) {
                cs.setConsumeOKTPS(ss.getTps());
            }
        }

        {
            StatsSnapshot ss = this.getConsumeFailedTPS(group, topic);
            if (ss != null) {
                cs.setConsumeFailedTPS(ss.getTps());
            }
        }

        {
            StatsSnapshot ss = this.topicAndGroupConsumeFailedTPS.getStatsDataInHour(topic + "@" + group);
            if (ss != null) {
                cs.setConsumeFailedMsgs(ss.getSum());
            }
        }

        return cs;
    }

    private StatsSnapshot getPullRT(final String group, final String topic) {
        return this.topicAndGroupPullRT.getStatsDataInMinute(topic + "@" + group);
    }

    private StatsSnapshot getPullTPS(final String group, final String topic) {
        return this.topicAndGroupPullTPS.getStatsDataInMinute(topic + "@" + group);
    }

    private StatsSnapshot getConsumeRT(final String group, final String topic) {
        StatsSnapshot statsData = this.topicAndGroupConsumeRT.getStatsDataInMinute(topic + "@" + group);
        if (0 == statsData.getSum()) {
            statsData = this.topicAndGroupConsumeRT.getStatsDataInHour(topic + "@" + group);
        }

        return statsData;
    }

    private StatsSnapshot getConsumeOKTPS(final String group, final String topic) {
        return this.topicAndGroupConsumeOKTPS.getStatsDataInMinute(topic + "@" + group);
    }

    private StatsSnapshot getConsumeFailedTPS(final String group, final String topic) {
        return this.topicAndGroupConsumeFailedTPS.getStatsDataInMinute(topic + "@" + group);
    }

    public void incPopRT(final String brokerName, final String group, final String topic, final long rt) {
        this.popRT.addValue(brokerName + ":" + topic + "@" + group, (int) rt, 1);
    }

    public void incPopQPS(final String brokerName, final String group, final String topic) {
        this.popQPS.addValue(brokerName + ":" + topic + "@" + group, 1, 1);
    }

    public void incPeekRT(final String brokerName, final String group, final String topic, final long rt) {
        this.peekRT.addValue(brokerName + ":" + topic + "@" + group, (int) rt, 1);
    }

    public void incPeekQPS(final String brokerName, final String group, final String topic) {
        this.peekQPS.addValue(brokerName + ":" + topic + "@" + group, 1, 1);
    }

    public void incAckRT(final String brokerName, final String group, final String topic, final long rt) {
        this.ackRT.addValue(brokerName + ":" + topic + "@" + group, (int) rt, 1);
    }

    public void incAckQPS(final String brokerName, final String group, final String topic) {
        this.ackQPS.addValue(brokerName + ":" + topic + "@" + group, 1, 1);
    }

    public void incChangeRT(final String brokerName, final String group, final String topic, final long rt) {
        this.changeRT.addValue(brokerName + ":" + topic + "@" + group, (int) rt, 1);
    }

    public void incChangeQPS(final String brokerName, final String group, final String topic) {
        this.changeQPS.addValue(brokerName + ":" + topic + "@" + group, 1, 1);
    }
}
