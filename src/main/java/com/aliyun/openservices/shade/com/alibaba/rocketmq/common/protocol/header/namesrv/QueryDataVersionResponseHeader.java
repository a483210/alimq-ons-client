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

package com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.header.namesrv;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.exception.RemotingCommandException;

public class QueryDataVersionResponseHeader implements CommandCustomHeader {
    @CFNotNull
    private Boolean changed;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public Boolean getChanged() {
        return changed;
    }

    public void setChanged(Boolean changed) {
        this.changed = changed;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("QueryDataVersionResponseHeader{");
        sb.append("changed=").append(changed);
        sb.append('}');
        return sb.toString();
    }
}
