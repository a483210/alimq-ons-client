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

package com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.consumer;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.log.ClientLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;
import com.google.common.base.Objects;
import io.netty.util.internal.ConcurrentSet;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MergeThreadExecutor {
    private final static InternalLogger LOG = ClientLogger.getLog();
    private final static long MAX_TIME_CONSUME_CONTINUOUSLY =
        Long.parseLong(System.getProperty("rocketmq.client.maxTimeConsumeContinuously", "5000"));
    private final ThreadPoolExecutor threadPoolExecutor;
    private final MessageQueueGroupLock queueGroupLock = new MessageQueueGroupLock();
    private final ConsumeMessageOrderlyByGroupService cs;
    private ConcurrentSet<ContinuouslyMergeRequest> mergeRequestSet = new ConcurrentSet<ContinuouslyMergeRequest>();

    private int mergeThreadMin = 10;

    private int mergeThreadMax = 32;

    public MergeThreadExecutor(ConsumeMessageOrderlyByGroupService cs) {
        this.cs = cs;
        this.threadPoolExecutor = new ThreadPoolExecutor(
            mergeThreadMin,
            mergeThreadMax,
            1000 * 60,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new ThreadFactoryImpl("MergeMessageThread_"));
    }

    public void shutdown()  {
        this.threadPoolExecutor.shutdown();
    }

    public void setCorePoolSize(int corePoolSize) {
        corePoolSize = Math.max(mergeThreadMin, corePoolSize);
        this.threadPoolExecutor.setCorePoolSize(corePoolSize);
    }

    public int getCorePoolSize() {
        return this.threadPoolExecutor.getCorePoolSize();
    }

    public MessageQueueGroupLock getQueueGroupLock() {
        return queueGroupLock;
    }

    public void remove(ContinuouslyMergeRequest continuouslyMergeRequest) {
        mergeRequestSet.remove(continuouslyMergeRequest);
    }

    public void submit(MergeRequest mergeRequest, boolean force) {
        mergeRequest.setInterrupted(false);
        mergeRequest.setInterruptCode(null);
        ContinuouslyMergeRequest continuouslyMergeRequest = new ContinuouslyMergeRequest(mergeRequest);
        boolean isNewReq = mergeRequestSet.add(continuouslyMergeRequest);
        if (force || isNewReq) {
            try {
                threadPoolExecutor.submit(continuouslyMergeRequest);
            } catch (Exception e) {
                LOG.error("error submit merge request: {}, mq: {}",
                    e.toString(), mergeRequest.getQueueGroup().getMessageQueueGroup());
            }
        }
    }

    public void submitLater(final MergeRequest mergeRequest, final long suspendTimeMillis) {
        long timeMillis = suspendTimeMillis;
        if (timeMillis == -1) {
            timeMillis = cs.getDefaultMQPushConsumer().getSuspendCurrentQueueTimeMillis();
        }

        if (timeMillis > 30000) {
            timeMillis = 30000;
        }

        cs.getScheduledExecutorService().schedule(new Runnable() {

            @Override
            public void run() {
                MergeThreadExecutor.this.submit(mergeRequest, true);
            }
        }, timeMillis, TimeUnit.MILLISECONDS);
    }

    public void tryLockLaterAndMergeAgain(final MergeRequest mergeRequest, final long delayMillis) {
        cs.getScheduledExecutorService().schedule(new Runnable() {
            @Override
            public void run() {
                boolean lockOK = cs.lockMQGroup(mergeRequest.getQueueGroup().getMessageQueueGroup());
                if (lockOK) {
                    MergeThreadExecutor.this.submitLater(mergeRequest, 10);
                } else {
                    MergeThreadExecutor.this.submitLater(mergeRequest, 3000);
                }
            }
        }, delayMillis, TimeUnit.MILLISECONDS);
    }

    public void updateLaterAndMergeAgain(final MergeRequest mergeRequest, final long delayMillis) {
        cs.getScheduledExecutorService().schedule(new Runnable() {
            @Override
            public void run() {
                cs.defaultMQPushConsumerImpl.getmQClientFactory().updateTopicRouteInfoFromNameServer(mergeRequest.getQueueGroup().getTopic());
                boolean rebalanced = cs.defaultMQPushConsumerImpl.doRebalance();
                QueueGroup oldQueueGroup = mergeRequest.getQueueGroup();
                mergeRequest.setQueueGroup(cs.getQueueGroupMap().get(oldQueueGroup.getTopic()).get(oldQueueGroup.getQueueGroupId()));
                if (rebalanced) {
                    MergeThreadExecutor.this.submitLater(mergeRequest, 10);
                } else {
                    MergeThreadExecutor.this.submitLater(mergeRequest, 3000);
                }
            }
        }, delayMillis, TimeUnit.MILLISECONDS);
    }

    private class ContinuouslyMergeRequest implements Runnable {
        private MergeRequest mergeRequest;

        public ContinuouslyMergeRequest(MergeRequest mergeRequest) {
            this.mergeRequest = mergeRequest;
        }

        @Override public void run() {
            try {
                if (mergeRequest.getQueueGroup().getQueuePairs() == null) {
                    MergeThreadExecutor.this.remove(this);
                    return;
                }
                ProcessQueueGroup.ProcessQueueGroupStatus pqGroupStatus = mergeRequest.getQueueGroup().getProcessQueueGroup().getProcessQueueStatus();
                if (pqGroupStatus == ProcessQueueGroup.ProcessQueueGroupStatus.ALL_DROPPED) {
                    LOG.warn("run, the message queue group not be able to consume, because it's dropped. {}", mergeRequest.getQueueGroup().getMessageQueueGroup());
                    MergeThreadExecutor.this.remove(this);
                    return;
                } else if (pqGroupStatus == ProcessQueueGroup.ProcessQueueGroupStatus.PARTIALLY_DROPPED) {
                    LOG.warn("run, the message queue group not be able to consume, because some of its message queues are dropped. {}", mergeRequest.getQueueGroup().getMessageQueueGroup());
                    updateLaterAndMergeAgain(mergeRequest, 1000);
                    return;
                }

                Object objLock = queueGroupLock.fetchLockObject(mergeRequest.getQueueGroup().getMessageQueueGroup());

                long beginTime = System.currentTimeMillis();
                for (int invokeCnt = 0; !mergeRequest.isInterrupted(); invokeCnt++) {
                    long interval = System.currentTimeMillis() - beginTime;
                    // let other QueueGroups have the chance to be merged, when num of QueueGroup is larger than thread pool size.
                    if (invokeCnt > 0 && interval > MAX_TIME_CONSUME_CONTINUOUSLY) {
                        submit(mergeRequest, true);
                        return;
                    }
                    synchronized (objLock) {
                        mergeRequest.run();
                    }
                }

                if (mergeRequest.getInterruptCode() == MergeRequest.InterruptCode.REMOVE_REQUEST) {
                    remove(this);

                    synchronized (objLock) {
                        // run task again, in case new MergeRequest failed to submit while this has been removed
                        mergeRequest.run();
                    }
                } else if (mergeRequest.getInterruptCode() == MergeRequest.InterruptCode.LOCK_LATER) {
                    tryLockLaterAndMergeAgain(mergeRequest, 10);
                } else if (mergeRequest.getInterruptCode() == MergeRequest.InterruptCode.UPDATE_LATER) {
                    updateLaterAndMergeAgain(mergeRequest, 10);
                } else if (mergeRequest.getInterruptCode() == MergeRequest.InterruptCode.MERGE_LATER) {
                    MergeThreadExecutor.this.submitLater(mergeRequest, 1);
                } else {
                    LOG.error("unexpected interrupt code in mergeRequest {}", mergeRequest.getInterruptCode());
                    remove(this);
                }
            } catch (Exception e) {
                LOG.error("Run merge request exception", e);
            }
        }

        @Override public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ContinuouslyMergeRequest request = (ContinuouslyMergeRequest) o;
            return Objects.equal(mergeRequest, request.mergeRequest);
        }

        @Override public int hashCode() {
            return Objects.hashCode(mergeRequest);
        }
    }
}
