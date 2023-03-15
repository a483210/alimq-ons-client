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
package com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.vtoa;

public class Vtoa {
    public int vid;
    public int vaddr;
    public int vport;

    public Vtoa() {
    }

    public Vtoa(int vid, int vaddr, int vport) {
        this.vid = vid;
        this.vaddr = vaddr;
        this.vport = vport;
    }

    public int getVid() {
        return vid;
    }

    public void setVid(int vid) {
        this.vid = vid;
    }

    public int getVaddr() {
        return vaddr;
    }

    public void setVaddr(int vaddr) {
        this.vaddr = vaddr;
    }

    public int getVport() {
        return vport;
    }

    public void setVport(int vport) {
        this.vport = vport;
    }
}