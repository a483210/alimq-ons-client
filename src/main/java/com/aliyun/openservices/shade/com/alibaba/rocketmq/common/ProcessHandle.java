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
package com.aliyun.openservices.shade.com.alibaba.rocketmq.common;

import com.sun.jna.platform.win32.Kernel32;

public class ProcessHandle {
    private static final String OS_NAME = System.getProperty("os.name");

    private static boolean isWindows() {
        return OS_NAME.toLowerCase().contains("windows");
    }

    public static int pid() {
        if (isWindows()) {
            return Kernel32.INSTANCE.GetCurrentProcessId();
        }
        return CLibrary.INSTANCE.getpid();
    }

}
