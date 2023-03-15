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

package com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.statistics;

import java.util.concurrent.atomic.AtomicInteger;

public class Histogram {
    private final String title;
    private final int grade;
    private final String[] labels;
    private final AtomicInteger[] values;

    public Histogram(String title, String[] labels) {
        this.title = title;
        this.labels = labels;
        this.grade = labels.length;
        values = new AtomicInteger[grade];
        for (int i = 0; i < grade; i++) {
            values[i] = new AtomicInteger(0);
        }
    }

    public void add(long index) {
        if (index < 0) {
            return;
        }

        if (index >= grade) {
            index = grade - 1;
        }

        values[(int) index].incrementAndGet();
    }

    public String report(boolean reset) {
        StringBuilder sb = new StringBuilder(title);
        sb.append(": [");
        for (int i = 0; i < grade; i++) {
            sb.append(labels[i]).append(": ");
            if (reset) {
                sb.append(values[i].getAndSet(0));
            } else {
                sb.append(values[i].get());
            }
            sb.append(", ");
        }
        sb.delete(sb.length() - 2, sb.length());
        sb.append("]");
        return sb.toString();
    }
}
