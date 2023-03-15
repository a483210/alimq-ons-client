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

import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQClientException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.log.ClientLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.Pair;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageAccessor;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageConst;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageExt;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueue;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;
import com.google.common.base.Objects;

import java.util.*;

public class MergeRequest implements Runnable {
    private final static InternalLogger LOG = ClientLogger.getLog();

    private final ConsumeMessageOrderlyByGroupService cs;

    // QueueGroup may be updated later, so no final here.
    private QueueGroup queueGroup;
    private InterruptCode interruptCode;

    private boolean isInterrupted = false;
    private int tryMergeCount = 0;

    public MergeRequest(QueueGroup queueGroup, ConsumeMessageOrderlyByGroupService cs) {
        this.queueGroup = queueGroup;
        this.cs = cs;
    }

    public void setInterrupted(boolean interrupted) {
        isInterrupted = interrupted;
    }

    public boolean isInterrupted() {
        return isInterrupted;
    }

    public InterruptCode getInterruptCode() {
        return interruptCode;
    }

    public void setInterruptCode(InterruptCode interruptCode) {
        this.interruptCode = interruptCode;
    }

    public enum InterruptCode {
        // remove this request
        REMOVE_REQUEST,
        // lock later and merge again
        LOCK_LATER,
        // update later and merge again
        UPDATE_LATER,
        // merge later
        MERGE_LATER,
    }

    @Override
    public void run() {
        if (MessageModel.BROADCASTING.equals(cs.getDefaultMQPushConsumerImpl().messageModel())
            || (this.queueGroup.getProcessQueueGroup().isLocked() && !this.queueGroup.getProcessQueueGroup().isLockExpired())) {

            if (queueGroup.getProcessQueueGroup().getProcessQueueStatus() != ProcessQueueGroup.ProcessQueueGroupStatus.NOT_DROPPED) {
                LOG.warn("the message queue group not be able to consume, because it's dropped. {}", this.queueGroup.getMessageQueueGroup());
                setInterruptCode(InterruptCode.REMOVE_REQUEST);
                setInterrupted(true);
                return;
            }
            if (MessageModel.CLUSTERING.equals(cs.getDefaultMQPushConsumerImpl().messageModel())
                && !this.queueGroup.getProcessQueueGroup().isLocked()) {
                LOG.warn("the message queue group not locked, so consume later, {}", this.queueGroup.getMessageQueueGroup());
                setInterruptCode(InterruptCode.LOCK_LATER);
                setInterrupted(true);
                return;
            }
            if (MessageModel.CLUSTERING.equals(cs.getDefaultMQPushConsumerImpl().messageModel())
                && this.queueGroup.getProcessQueueGroup().isLockExpired()) {
                LOG.warn("the message queue group lock expired, so consume later, {}", this.queueGroup.getMessageQueueGroup());
                setInterruptCode(InterruptCode.LOCK_LATER);
                setInterrupted(true);
                return;
            }

            List<Pair<QueuePair, List<MessageExt>>> msgPairList = tryMergeMessage(queueGroup.getQueuePairs());

            if (msgPairList == null) {
                setInterruptCode(InterruptCode.UPDATE_LATER);
                setInterrupted(true);
                return;
            }
            // merge returns empty result
            if (msgPairList.isEmpty()) {
                // All of process queues in group are empty
                if (queueGroup.isAllQueueGroupEmpty()) {
                    setInterruptCode(InterruptCode.REMOVE_REQUEST);
                    setInterrupted(true);
                }
                // try next merge
                return;
            }

            cs.submitConsumeRequest(msgPairList, queueGroup);
        } else {
            if (this.queueGroup.getProcessQueueGroup().getProcessQueueStatus() != ProcessQueueGroup.ProcessQueueGroupStatus.NOT_DROPPED) {
                LOG.warn("the message queue group not be able to consume, because it's dropped. {}", this.queueGroup.getMessageQueueGroup());
                setInterruptCode(InterruptCode.REMOVE_REQUEST);
                setInterrupted(true);
                return;
            }
            LOG.warn("the message queue group not locked or lock expired, so consume later, {}", this.queueGroup.getMessageQueueGroup());
            setInterruptCode(InterruptCode.LOCK_LATER);
            setInterrupted(true);
        }
    }

    List<Pair<QueuePair, List<MessageExt>>> tryMergeMessage(Set<QueuePair> queuesToMerge) {
        tryMergeCount++;
        List<Pair<QueuePair, List<MessageExt>>> msgPairList = new ArrayList<Pair<QueuePair, List<MessageExt>>>();
        List<QueuePair> queuePairsToCheck = new ArrayList<QueuePair>();
        for (QueuePair queuePair : queuesToMerge) {
            long unmergedMessageSize = queuePair.getProcessQueue().unmergedMessageSize();

            // have msgs not merged
            if (unmergedMessageSize > 0) {
                queuePair.getProcessQueue().setSafeMergeSize(unmergedMessageSize);
            } else {
                queuePairsToCheck.add(queuePair);
            }
        }

        if (queuePairsToCheck.size() == queuesToMerge.size()) {
            return msgPairList;
        }

        if (queuePairsToCheck.size() > 0 && tryMergeCount <= 1) {
            setInterruptCode(InterruptCode.MERGE_LATER);
            setInterrupted(true);
            return msgPairList;
        }

        for (QueuePair queuePair : queuePairsToCheck) {
            // need to ensure queues are empty before submit safe point to consume
            try {
                long maxOffset = cs.getDefaultMQPushConsumerImpl().maxOffset(queuePair.getMessageQueue(), false);
                long nextOffset = queuePair.getProcessQueue().getNextOffset();
                long unmergedMessageSize = queuePair.getProcessQueue().unmergedMessageSize();

                if (maxOffset > nextOffset || unmergedMessageSize > 0) {
                    // new msg arrived, wait for next time merge
                    return msgPairList;
                } else {
                    // forbid the queue to merge
                    queuePair.getProcessQueue().setSafeMergeSize(0);
                }
            } catch (MQClientException e) {
                LOG.error("tryMergeMessage check maxOffset exception", e);
                return msgPairList;
            }
        }

        msgPairList = mergeMessage(queuesToMerge);
        tryMergeCount = 0;

        return msgPairList;
    }

    private List<Pair<QueuePair, List<MessageExt>>> mergeMessage(Set<QueuePair> queuesToMerge) {
        List<Pair<QueuePair, List<MessageExt>>> msgPairList = new ArrayList<Pair<QueuePair, List<MessageExt>>>();
        Pair<QueuePair, List<MessageExt>> messagePair = null;

        Map<String, List<MessageExt>> messageMap = new HashMap<String, List<MessageExt>>();
        Map<String, QueuePair> queueMap = new HashMap<String, QueuePair>();
        Map<String, Integer> localProgressMap = new HashMap<String, Integer>();

        for (QueuePair queuePair : queuesToMerge) {
            List<MessageExt> pqMsgList;
            pqMsgList = queuePair.getProcessQueue().peekMessagesToMerge();
            if (pqMsgList.isEmpty()) {
                continue;
            }
            String key = MessageConst.withPrefix(MessageConst.PREFIX_HA_ORDER_MQ_OFFSET, queuePair.getMessageQueue().generateKey());
            // local progress for each queue starts from 0
            localProgressMap.put(key, 0);
            queueMap.put(key, queuePair);
            messageMap.put(key, pqMsgList);
        }

        if (messageMap.isEmpty()) {
            return msgPairList;
        }

        if (!isQueueGroupSizeValid(messageMap)) {
            return null;
        }

        if (messageMap.size() == 1) {
            // if there is only one queue to merge, return directly.
            QueuePair queuePair = queueMap.values().iterator().next();
            List<MessageExt> msgList = messageMap.values().iterator().next();
            queuePair.getProcessQueue().getMergeProgress().addAndGet(msgList.size());
            msgPairList.add(new Pair<QueuePair, List<MessageExt>>(queuePair, msgList));

            return msgPairList;
        }

        List<MessageExtWrapper> sortedMsgList = new ArrayList<MessageExtWrapper>();

        // the list to hold all the head message for each queue.
        List<MessageExtWrapper> tmpSortList = new LinkedList<MessageExtWrapper>();
        for (String key : messageMap.keySet()) {
            tmpSortList.add(new MessageExtWrapper(key, messageMap.get(key).get(0)));
        }

        MessageExtWrapper earliestMsg;

        while (!tmpSortList.isEmpty()) {
            // poll the earliest message from all head messages.
            earliestMsg = pollEarliestMsg(tmpSortList);
            sortedMsgList.add(earliestMsg);

            String mqKey = earliestMsg.getKey();

            int sortProgress = localProgressMap.get(mqKey) + 1;

            if (sortProgress == messageMap.get(mqKey).size()) {
                // one queue is running out of msg, continue to merge remaining messages.
                continue;
            }
            // add the next head message to the list.
            tmpSortList.add(new MessageExtWrapper(mqKey, messageMap.get(mqKey).get(sortProgress)));
            // update sort progress
            localProgressMap.put(mqKey, sortProgress);
        }

        // sorting complete, reset progress map.
        for (String key : localProgressMap.keySet()) {
            localProgressMap.put(key, 0);
        }

        MessageQueue previousMq = null;

        // construct queue pair for consuming from sorted list.
        for (MessageExtWrapper msgWrapper : sortedMsgList) {
            String mqKey = msgWrapper.getKey();
            if (previousMq == null) {
                previousMq = queueMap.get(mqKey).getMessageQueue();
                messagePair = new Pair<QueuePair, List<MessageExt>>(queueMap.get(mqKey), new ArrayList<MessageExt>());
            }
            if (!queueMap.get(mqKey).getMessageQueue().equals(previousMq)) {
                // current mq is different from previous mq
                msgPairList.add(messagePair);
                previousMq = queueMap.get(mqKey).getMessageQueue();
                messagePair = new Pair<QueuePair, List<MessageExt>>(queueMap.get(mqKey), new ArrayList<MessageExt>());
            }
            // add msg to the list in message pair
            messagePair.getObject2().add(msgWrapper.getMsgExt());
            int mergeProgress = localProgressMap.get(mqKey) + 1;
            ProcessQueue pq = queueMap.get(mqKey).getProcessQueue();
            // update merge progress of process queue, this marks the message in this process queue been 'officially' merged.
            pq.getMergeProgress().incrementAndGet();
            if (mergeProgress == messageMap.get(mqKey).size()) {
                // one queue is running out of msg in this time merging, submit to consume and wait for next merge operation.
                msgPairList.add(messagePair);
                break;
            }
            // update local merge progress
            localProgressMap.put(mqKey, mergeProgress);
        }
        return msgPairList;
    }

    private boolean isQueueGroupSizeValid(Map<String, List<MessageExt>> msgMap) {
        for (String mqKey : msgMap.keySet()) {
            List<MessageExt> msgs = msgMap.get(mqKey);

            for (MessageExt msg : msgs) {
                String queueGroupInfo = MessageAccessor.getQueueGroupSnapshot(msg);
                String[] mqKeys = new String[] {};
                if (queueGroupInfo != null) {
                    mqKeys = queueGroupInfo.split(";");
                }
                int queueGroupSize = queueGroup.getMessageQueueGroup().getMessageQueueList().size();
                if (mqKeys.length > queueGroupSize) {
                    LOG.warn("topic: {}, group id: {}, queue group size {} not match msg property {}, maybe it has been changed",
                        queueGroup.getTopic(), queueGroup.getQueueGroupId(), queueGroupSize, mqKeys.length);
                    return false;
                }
            }
        }
        return true;
    }

    public static MessageExtWrapper pollEarliestMsg(List<MessageExtWrapper> msgsToSort) {
        List<MessageExtWrapper> candidates = new LinkedList<MessageExtWrapper>();
        MessageExtWrapper earliestMsgByTime = null;
        MessageExtWrapper globalEarliestMsgByTime = null;
        for (int i = 0; i < msgsToSort.size(); i++) {
            final MessageExtWrapper thisMsg = msgsToSort.get(i);

            if (globalEarliestMsgByTime == null || thisMsg.getMsgExt().getStoreTimestamp() < globalEarliestMsgByTime.getMsgExt().getStoreTimestamp()) {
                globalEarliestMsgByTime = thisMsg;
            }

            boolean candidateFlag = true;
            for (int j = 0; j < msgsToSort.size(); j++) {
                final MessageExtWrapper thatMsg = msgsToSort.get(j);
                if (j == i) {
                    continue;
                }
                // This message can't be a candidate message if it's later than any message
                if (compareByOffset(thisMsg, thatMsg) > 0) {
                    candidateFlag = false;
                    break;
                }
            }

            if (candidateFlag) {
                if (earliestMsgByTime == null || thisMsg.getMsgExt().getStoreTimestamp() < earliestMsgByTime.getMsgExt().getStoreTimestamp()) {
                    earliestMsgByTime = thisMsg;
                }
                candidates.add(thisMsg);
            }
        }

        if (!candidates.isEmpty()) {
            earliestMsgByTime = candidates.size() == 1 ? candidates.get(0) : earliestMsgByTime;
        } else {
            LOG.warn("[BUG] pollEarliestMsg candidates empty, msgsToSort {}", msgsToSort);
            earliestMsgByTime = globalEarliestMsgByTime;
        }

        msgsToSort.remove(earliestMsgByTime);

        return earliestMsgByTime;
    }

    public static class MessageExtWrapper {
        private String key;
        private MessageExt msgExt;

        public MessageExtWrapper(String key, MessageExt msgExt) {
            this.key = key;
            this.msgExt = msgExt;
        }

        public String getKey() {
            return key;
        }

        public MessageExt getMsgExt() {
            return msgExt;
        }

        @Override
        public String toString() {
            return "MessageExtWarpper [key =" + key + ", toString()=" + msgExt.toString() + "]";
        }
    }

    /**
     * compare two messages by offset
     *
     * @param o1 message to compare
     * @param o2 another message to compare
     * @return 0 if not comparable by offset, 1 if o1 is later than o2, -1 if o2 is later than o1.
     */
    private static int compareByOffset(MessageExtWrapper o1, MessageExtWrapper o2) {
        long otherOffsetOnThis = getOffsetOn(o1, o2);
        long thisOffsetOnOther = getOffsetOn(o2, o1);

        long thisMsgCount = o1.getMsgExt().getQueueOffset() + 1;
        long otherMsgCount = o2.getMsgExt().getQueueOffset() + 1;

        if (otherOffsetOnThis >= otherMsgCount) {
            // other comes first
            return 1;
        } else if (thisOffsetOnOther >= thisMsgCount) {
            // this comes first
            return -1;
        } else {
            return 0;
        }
    }

    private static long getOffsetOn(MessageExtWrapper o1, MessageExtWrapper o2) {
        long otherOffsetOnThis;
        try {
            if (o1.getMsgExt().getProperties().containsKey(o2.key)) {
                otherOffsetOnThis = Long.parseLong(o1.getMsgExt().getProperties().get(o2.key));
            } else {
                otherOffsetOnThis = -1;
            }
        } catch (NumberFormatException e) {
            otherOffsetOnThis = -1;
        }
        return otherOffsetOnThis;
    }

    public QueueGroup getQueueGroup() {
        return queueGroup;
    }

    public void setQueueGroup(QueueGroup queueGroup) {
        this.queueGroup = queueGroup;
    }

    @Override public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MergeRequest that = (MergeRequest) o;
        return Objects.equal(queueGroup, that.queueGroup);
    }

    @Override public int hashCode() {
        return Objects.hashCode(queueGroup);
    }
}
