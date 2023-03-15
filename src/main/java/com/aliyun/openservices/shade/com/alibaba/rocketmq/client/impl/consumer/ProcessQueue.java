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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.log.ClientLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueue;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.utils.MessageUtils;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageAccessor;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageConst;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageExt;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.body.ProcessQueueInfo;

/**
 * Queue consumption snapshot
 */
public class ProcessQueue {
    public final static long REBALANCE_LOCK_MAX_LIVE_TIME =
        Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockMaxLiveTime", "30000"));
    public final static long REBALANCE_LOCK_INTERVAL = Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockInterval", "20000"));
    private final static long PULL_MAX_IDLE_TIME = Long.parseLong(System.getProperty("rocketmq.client.pull.pullMaxIdleTime", "120000"));
    private final InternalLogger log = ClientLogger.getLog();
    private final ReadWriteLock lockTreeMap = new ReentrantReadWriteLock();
    /**
     * message list, contains message in waiting & consuming list
     */
    private final TreeMap<Long, MessageExt> msgTreeMap = new TreeMap<Long, MessageExt>();
    private final AtomicLong msgCount = new AtomicLong();
    private final AtomicLong msgSize = new AtomicLong();
    private final AtomicLong mergeProgress = new AtomicLong();
    private final AtomicLong consumeProgress = new AtomicLong();
    private final ReadWriteLock lockConsume = new ReentrantReadWriteLock();
    /**
     * message consuming list, A subset of msgTreeMap, will only be used when orderly consume
     */
    private final TreeMap<Long, MessageExt> consumingMsgOrderlyTreeMap = new TreeMap<Long, MessageExt>();

    /**
     * partitional message waiting list separated by sharding key index, will only be used when orderly consume
     */
    private final PartitionalMessageList partitionalMessageList;

    private final AtomicLong tryUnlockTimes = new AtomicLong(0);
    private volatile long safeMergeSize = 0;
    private volatile long nextOffset = 0L; // next offset to pull msg from broker
    private volatile long queueOffsetMax = 0L; // max offset of the msg that existed in the ProcessQueue
    private volatile boolean dropped = false;
    private volatile long lastPullTimestamp = System.currentTimeMillis();
    private volatile long lastConsumeTimestamp = System.currentTimeMillis();
    private volatile boolean locked = false;
    private volatile long lastLockTimestamp = System.currentTimeMillis();
    private volatile long msgAccCnt = 0;
    private volatile boolean isNormalMsgClean = true;
    private volatile boolean isReceivedHAMsg = false;

    /**
     * Associated message queue of this ProcessQueue, which may be null.
     */
    private MessageQueue messageQueue;

    public ProcessQueue() {
        this.partitionalMessageList = new PartitionalMessageList(1);
    }

    public ProcessQueue(int shardNum) {
        this.partitionalMessageList = new PartitionalMessageList(shardNum);
    }

    public boolean isLockExpired() {
        return (System.currentTimeMillis() - this.lastLockTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;
    }

    public boolean isPullExpired() {
        return (System.currentTimeMillis() - this.lastPullTimestamp) > PULL_MAX_IDLE_TIME;
    }

    /**
     * @param pushConsumer
     */
    public void cleanExpiredMsg(DefaultMQPushConsumer pushConsumer) {
        if (pushConsumer.getDefaultMQPushConsumerImpl().isConsumeOrderly()) {
            return;
        }

        int loop = Math.min(msgTreeMap.size(), 16);
        for (int i = 0; i < loop; i++) {
            MessageExt msg = null;
            try {
                this.lockTreeMap.readLock().lockInterruptibly();
                try {
                    if (msgTreeMap.isEmpty()) {
                        break;
                    }
                    String consumeStartTimestamp = MessageAccessor.getConsumeStartTimeStamp(msgTreeMap.firstEntry().getValue());
                    if (consumeStartTimestamp == null
                        || System.currentTimeMillis() - Long.parseLong(consumeStartTimestamp) <= pushConsumer.getConsumeTimeout() * 60 * 1000) {
                        break;
                    }

                    msg = msgTreeMap.firstEntry().getValue();
                } finally {
                    this.lockTreeMap.readLock().unlock();
                }
            } catch (Exception e) {
                log.error("getExpiredMsg exception", e);
            }

            if (null == msg) {
                log.warn("Failed to peek first message from tree-map, possibly due to concurrent remove");
                return;
            }

            try {
                pushConsumer.sendMessageBack(msg, 0);
                log.info("Expired msg sent back. topic={}, msgId={}, storeHost={}, queueId={}, queueOffset={}",
                    msg.getTopic(), msg.getMsgId(), msg.getStoreHost(), msg.getQueueId(), msg.getQueueOffset());
                try {
                    this.lockTreeMap.writeLock().lockInterruptibly();
                    try {
                        if (!msgTreeMap.isEmpty() && msg.getQueueOffset() == msgTreeMap.firstKey()) {
                            try {
                                long offset = removeMessage(Collections.singletonList(msg));
                                if (offset > 0 && null != messageQueue) {
                                    log.info("New offset after removal of expired message: {}", offset);
                                    // pushConsumer.getOffsetStore().updateOffset(messageQueue, offset, true);
                                }
                            } catch (Exception e) {
                                log.error("Remove messages from tree-map raised an exception", e);
                            }
                        }
                    } finally {
                        this.lockTreeMap.writeLock().unlock();
                    }
                } catch (InterruptedException e) {
                    log.error("getExpiredMsg exception", e);
                }
            } catch (Exception e) {
                log.error("send expired msg exception", e);
            }
        }
    }

    public void putMessage(final List<MessageExt> msgs) {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                int validMsgCnt = 0;
                for (MessageExt msg : msgs) {
                    MessageExt old = msgTreeMap.put(msg.getQueueOffset(), msg);
                    if (null == old) {
                        validMsgCnt++;
                        this.queueOffsetMax = Math.max(queueOffsetMax, msg.getQueueOffset());
                        msgSize.addAndGet(msg.getBody() != null ? msg.getBody().length : 0);
                    }

                    // put msg classify by sharding key
                    partitionalMessageList.putMessage(msg);
                }
                msgCount.addAndGet(validMsgCnt);

                if (!msgs.isEmpty()) {
                    MessageExt messageExt = msgs.get(msgs.size() - 1);
                    String property = messageExt.getProperty(MessageConst.PROPERTY_MAX_OFFSET);
                    if (property != null) {
                        long accTotal = Long.parseLong(property) - messageExt.getQueueOffset();
                        if (accTotal > 0) {
                            this.msgAccCnt = accTotal;
                        }
                    }
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("putMessage exception", e);
        }
    }

    public long getMaxSpan() {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    return this.msgTreeMap.lastKey() - this.msgTreeMap.firstKey();
                }
            } finally {
                this.lockTreeMap.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getMaxSpan exception", e);
        }

        return 0;
    }

    public long removeMessage(final List<MessageExt> msgs) {
        return removeMessage(msgs, -1);
    }

    public long removeMessage(final List<MessageExt> msgs, int shardingKeyIndex) {
        if (msgs == null || msgs.isEmpty()) {
            return -1;
        }

        final long now = System.currentTimeMillis();
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            this.lastConsumeTimestamp = now;
            try {
                if (!msgTreeMap.isEmpty()) {
                    int removedCnt = 0;
                    for (MessageExt msg : msgs) {
                        MessageExt prev = msgTreeMap.remove(msg.getQueueOffset());
                        if (prev != null) {
                            removedCnt--;
                            msgSize.addAndGet(msg.getBody() != null ? -msg.getBody().length : 0);
                        }

                        partitionalMessageList.removeMessage(msg, shardingKeyIndex);
                        consumingMsgOrderlyTreeMap.remove(msg.getQueueOffset());
                    }
                    msgCount.addAndGet(removedCnt);
                    mergeProgress.addAndGet(removedCnt);
                    consumeProgress.addAndGet(removedCnt);
                    return msgTreeMap.isEmpty() ? (this.queueOffsetMax + 1) : msgTreeMap.firstKey();
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (Throwable t) {
            log.error("removeMessage exception", t);
        }

        return -1;
    }

    public boolean hasMessage(final MessageExt msg) {
        if (msg == null) {
            return false;
        }

        try {
            this.lockTreeMap.readLock().lockInterruptibly();
            try {
                if (!msgTreeMap.isEmpty()) {
                    return msgTreeMap.containsKey(msg.getQueueOffset());
                }
            } finally {
                this.lockTreeMap.readLock().unlock();
            }
        } catch (Throwable t) {
            log.error("check has message exception", t);
        }

        return false;
    }

    public TreeMap<Long, MessageExt> getMsgTreeMap() {
        return msgTreeMap;
    }

    public AtomicLong getMsgCount() {
        return msgCount;
    }

    public AtomicLong getMsgSize() {
        return msgSize;
    }

    public AtomicLong getMergeProgress() {
        return mergeProgress;
    }

    public AtomicLong getConsumeProgress() {
        return consumeProgress;
    }

    public boolean isDropped() {
        return dropped;
    }

    public void setDropped(boolean dropped) {
        this.dropped = dropped;
    }

    public boolean isLocked() {
        return locked;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    public boolean isNormalMsgClean() {
        return isNormalMsgClean;
    }

    public void setNormalMsgClean(boolean normalMsgClean) {
        isNormalMsgClean = normalMsgClean;
    }

    public boolean isReceivedHAMsg() {
        return isReceivedHAMsg;
    }

    public void setReceivedHAMsg(boolean receivedHAMsg) {
        isReceivedHAMsg = receivedHAMsg;
    }

    // rollback, orderly service only
    public void rollback(List<MessageExt> msgs) {
        makeMessageToConsumeAgain(msgs);
    }

    public void rollback(List<MessageExt> msgs, int shardingKeyIndex) {
        makeMessageToConsumeAgain(msgs, shardingKeyIndex);
    }

    // get commit offset, for orderly service only
    public long commit(List<MessageExt> msgs) {
        return removeMessage(msgs);
    }

    public long commit(List<MessageExt> msgs, int shardingKeyIndex) {
        return removeMessage(msgs, shardingKeyIndex);
    }

    // make message consumable again, for orderly service only
    public void makeMessageToConsumeAgain(List<MessageExt> msgs) {
        makeMessageToConsumeAgain(msgs, -1);
    }

    public void makeMessageToConsumeAgain(List<MessageExt> msgs, int shardingKeyIndex) {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                for (MessageExt msg : msgs) {
                    this.msgTreeMap.put(msg.getQueueOffset(), msg);
                    this.consumingMsgOrderlyTreeMap.remove(msg.getQueueOffset());
                    this.partitionalMessageList.putMessage(msg, shardingKeyIndex);
                }
                if (consumeProgress.longValue() <= msgs.size()) {
                    consumeProgress.set(0L);
                } else {
                    consumeProgress.addAndGet(-msgs.size());
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("makeMessageToConsumeAgain exception", e);
        }
    }

    /**
     * Take (but not remove) messages to merge.
     * @return messages
     */
    public List<MessageExt> peekMessagesToMerge() {
        List<MessageExt> msgList;
        this.lockTreeMap.readLock().lock();
        try {
            int mergeProgress = this.getMergeProgress().intValue();
            msgList = new ArrayList<MessageExt>(msgTreeMap.values());
            msgList = msgList.subList(mergeProgress, (int) safeMergeSize + mergeProgress);
        } finally {
            this.lockTreeMap.readLock().unlock();
        }
        return msgList;
    }

    public List<MessageExt> takeMessages(final int batchSize) {
        return takeMessagesByShardingKeyIndex(0, batchSize);
    }

    // take messages, for orderly service only
    public List<MessageExt> takeMessagesByShardingKeyIndex(final int shardingKeyIndex, final int batchSize) {
        List<MessageExt> result = new ArrayList<MessageExt>();
        final long now = System.currentTimeMillis();
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            this.lastConsumeTimestamp = now;
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    result = partitionalMessageList.pollMessages(shardingKeyIndex, batchSize);

                    for (MessageExt msg : result) {
                        //msgTreeMap.remove(msg.getQueueOffset());
                        consumingMsgOrderlyTreeMap.put(msg.getQueueOffset(), msg);
                    }

                    consumeProgress.addAndGet(result.size());
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("take Messages exception", e);
        }

        return result;
    }

    public boolean hasTempMessage() {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();
            try {
                return !this.msgTreeMap.isEmpty();
            } finally {
                this.lockTreeMap.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("check temp messages exception", e);
        }

        return true;
    }

    public long unmergedMessageSize() {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();
            try {
                return msgCount.get() - mergeProgress.get();
            } finally {
                this.lockTreeMap.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("check unmerged message exception", e);
        }

        return 0L;
    }

    public void clear() {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                this.msgTreeMap.clear();
                this.consumingMsgOrderlyTreeMap.clear();
                this.partitionalMessageList.clear();
                this.msgCount.set(0);
                this.msgSize.set(0);
                this.queueOffsetMax = 0L;
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    public long getNextOffset() {
        return nextOffset;
    }

    public void setNextOffset(long nextOffset) {
        this.nextOffset = nextOffset;
    }

    public void setSafeMergeSize(long safeMergeSize) {
        this.safeMergeSize = safeMergeSize;
    }

    public long getQueueOffsetMax() {
        return queueOffsetMax;
    }

    public long getLastLockTimestamp() {
        return lastLockTimestamp;
    }

    public void setLastLockTimestamp(long lastLockTimestamp) {
        this.lastLockTimestamp = lastLockTimestamp;
    }

    public ReadWriteLock getLockConsume() {
        return lockConsume;
    }

    public long getLastPullTimestamp() {
        return lastPullTimestamp;
    }

    public void setLastPullTimestamp(long lastPullTimestamp) {
        this.lastPullTimestamp = lastPullTimestamp;
    }

    public long getMsgAccCnt() {
        return msgAccCnt;
    }

    public void setMsgAccCnt(long msgAccCnt) {
        this.msgAccCnt = msgAccCnt;
    }

    public long getTryUnlockTimes() {
        return this.tryUnlockTimes.get();
    }

    public void incTryUnlockTimes() {
        this.tryUnlockTimes.incrementAndGet();
    }

    public void fillProcessQueueInfo(final ProcessQueueInfo info) {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();

            if (!this.msgTreeMap.isEmpty()) {
                info.setCachedMsgMinOffset(this.msgTreeMap.firstKey());
                info.setCachedMsgMaxOffset(this.msgTreeMap.lastKey());
                info.setCachedMsgCount(this.msgTreeMap.size());
                info.setCachedMsgSizeInMiB((int) (this.msgSize.get() / (1024 * 1024)));

                int cnt = 0;
                for (Map.Entry<Long, MessageExt> entry : this.msgTreeMap.entrySet()) {
                    if (cnt++ == mergeProgress.get()) {
                        info.setMergeOffset(entry.getKey() + 1);
                        break;
                    }
                }
            } else {
                info.setMergeOffset(info.getCommitOffset());
            }

            if (!this.consumingMsgOrderlyTreeMap.isEmpty()) {
                info.setTransactionMsgMinOffset(this.consumingMsgOrderlyTreeMap.firstKey());
                info.setTransactionMsgMaxOffset(this.consumingMsgOrderlyTreeMap.lastKey());
                info.setTransactionMsgCount(this.consumingMsgOrderlyTreeMap.size());
            }

            info.setLocked(this.locked);
            info.setTryUnlockTimes(this.tryUnlockTimes.get());
            info.setLastLockTimestamp(this.lastLockTimestamp);

            info.setDropped(this.dropped);
            info.setLastPullTimestamp(this.lastPullTimestamp);
            info.setLastConsumeTimestamp(this.lastConsumeTimestamp);
        } catch (Exception e) {
        } finally {
            this.lockTreeMap.readLock().unlock();
        }
    }

    public long getLastConsumeTimestamp() {
        return lastConsumeTimestamp;
    }

    public void setLastConsumeTimestamp(long lastConsumeTimestamp) {
        this.lastConsumeTimestamp = lastConsumeTimestamp;
    }

    public void setMessageQueue(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    public static class PartitionalMessageList {
        private final ArrayList<TreeMap<Long, MessageExt>> partitionalMsgMapList;

        public PartitionalMessageList(int shardNum) {
            partitionalMsgMapList = new ArrayList<TreeMap<Long, MessageExt>>(shardNum);
            for (int i = 0; i < shardNum; i++) {
                partitionalMsgMapList.add(new TreeMap<Long, MessageExt>());
            }
        }

        private String getShardingKey(MessageExt msg) {
            String shardingKey = msg.getProperty(MessageConst.PROPERTY_SHARDING_KEY);
            if (shardingKey == null) {
                shardingKey = "";
            }
            return shardingKey;
        }

        private int getShardIndex(MessageExt msg) {
            // use different hash function with order producer
            String shardingKey = getShardingKey(msg);
            return MessageUtils.getShardingKeyIndex(shardingKey, partitionalMsgMapList.size());
        }

        public void putMessage(MessageExt msg) {
            putMessage(msg, -1);
        }

        public void putMessage(MessageExt msg, int shardIndex) {
            if (shardIndex < 0 || shardIndex >= partitionalMsgMapList.size()) {
                shardIndex = getShardIndex(msg);
            }

            TreeMap<Long, MessageExt> msgMap = partitionalMsgMapList.get(shardIndex);
            if (msgMap == null) {
                msgMap = new TreeMap<Long, MessageExt>();
                partitionalMsgMapList.set(shardIndex, msgMap);
            }

            msgMap.put(msg.getQueueOffset(), msg);
        }

        public void removeMessage(MessageExt msg) {
            removeMessage(msg, -1);
        }

        public void removeMessage(MessageExt msg, int shardIndex) {
            if (shardIndex < 0 || shardIndex >= partitionalMsgMapList.size()) {
                shardIndex = getShardIndex(msg);
            }

            TreeMap<Long, MessageExt> msgMap = partitionalMsgMapList.get(shardIndex);
            if (msgMap == null) {
                return;
            }

            msgMap.remove(msg.getQueueOffset());
        }

        public List<MessageExt> pollMessages(final int shardIndex, final int batchSize) {
            List<MessageExt> msgs = new ArrayList<MessageExt>(batchSize);
            TreeMap<Long, MessageExt> msgMap = partitionalMsgMapList.get(shardIndex);
            if (msgMap != null) {
                for (int i = 0; i < batchSize; i++) {
                    Map.Entry<Long, MessageExt> entry = msgMap.pollFirstEntry();
                    if (entry == null) {
                        break;
                    }

                    msgs.add(entry.getValue());
                }
            }
            return msgs;
        }

        public void clear() {
            for (TreeMap<Long, MessageExt> m : partitionalMsgMapList) {
                if (m != null) {
                    m.clear();
                }
            }
        }


    }
}
