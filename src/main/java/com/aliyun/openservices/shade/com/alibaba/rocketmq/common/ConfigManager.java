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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.constant.LoggerName;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLoggerFactory;
import com.google.common.io.Files;

public abstract class ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    public boolean load() {
        String fileName = null;
        try {
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName);

            if (null == jsonString || jsonString.length() == 0) {
                return this.loadBak();
            } else {
                this.decode(jsonString);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " failed, and try to load backup file", e);
            return this.loadBak();
        }
    }

    public abstract String configFilePath();

    private boolean loadBak() {
        String fileName = null;
        try {
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName + ".bak");
            if (jsonString != null && jsonString.length() > 0) {
                this.decode(jsonString);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " Failed", e);
            return false;
        }

        return true;
    }

    public abstract void decode(final String jsonString);

    protected abstract void write0(Writer writer);

    public synchronized void persist() {
        String config = configFilePath();
        String temp = config + ".tmp";
        String backup = config + ".bak";
        BufferedWriter bufferedWriter = null;
        try {
            File tmpFile = new File(temp);
            File parentDirectory = tmpFile.getParentFile();
            if (!parentDirectory.exists()) {
                if (!parentDirectory.mkdirs()) {
                    log.error("Failed to create directory: {}", parentDirectory.getCanonicalPath());
                    return;
                }
            }

            if (!tmpFile.exists()) {
                if (!tmpFile.createNewFile()) {
                    log.error("Failed to create file: {}", tmpFile.getCanonicalPath());
                    return;
                }
            }
            bufferedWriter = new BufferedWriter(new FileWriter(tmpFile, false));
            write0(bufferedWriter);
            bufferedWriter.flush();
            log.debug("Finished writing tmp file: {}", temp);

            File configFile = new File(config);
            if (configFile.exists()) {
                Files.copy(configFile, new File(backup));
                configFile.delete();
            }

            tmpFile.renameTo(configFile);
        } catch (IOException e) {
            log.error("Failed to persist {}", temp, e);
        } finally {
            if (null != bufferedWriter) {
                try {
                    bufferedWriter.close();
                } catch (IOException ignore) {
                }
            }
        }
    }
}
