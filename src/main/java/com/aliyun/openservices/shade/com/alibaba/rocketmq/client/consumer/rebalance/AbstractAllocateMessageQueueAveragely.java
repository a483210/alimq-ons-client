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

package com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractAllocateMessageQueueAveragely<T> extends AbstractAllocateMessageQueueStrategy {
    public List<T> allocateAveragely(String currentCID, List<String> cidAll, List<T> queueAll) {
        List<T> result = new ArrayList<T>();

        int index = cidAll.indexOf(currentCID);
        int mod = queueAll.size() % cidAll.size();
        int averageSize =
            queueAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? queueAll.size() / cidAll.size()
                + 1 : queueAll.size() / cidAll.size());
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        int range = Math.min(averageSize, queueAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(queueAll.get((startIndex + i) % queueAll.size()));
        }

        return result;
    }
}
