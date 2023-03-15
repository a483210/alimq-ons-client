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

package com.aliyun.openservices.shade.com.alibaba.rocketmq.common.metrics;

import io.prometheus.client.*;
import io.prometheus.client.exporter.common.TextFormat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MetricsUtils {

    public static final CollectorRegistry REGISTRY = new CollectorRegistry();

    private static final ConcurrentMap<String, Collector> COLLECTOR_MAP = new ConcurrentHashMap<String, Collector>();

    private static final LinkedHashSet<String> TOKENS = new LinkedHashSet<String>();

    public static final int COMPRESSION_MIN_SIZE = 10000;

    private static final int TOKEN_MAX_NUM = 100;

    /* Customize our own buckets for histogram */
    private static final double[] BUCKETS = new double[] {0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75,
        1, 2.5, 5, 7.5, 10, 30, 60};

    public static String[] labels2StringArray(Set<String> labels) {
        return labels.toArray(new String[labels.size()]);
    }

    public static String[] values2StringArray(ArrayList<String> values) {
        return values.toArray(new String[values.size()]);
    }

    public static boolean addTokens(String token) {
        if (TOKENS.contains(token)) {
            return false;
        }
        synchronized (TOKENS) {
            TOKENS.add(token);
            int tokenSize = TOKENS.size();
            int needDeletedNum = tokenSize - TOKEN_MAX_NUM;
            Iterator<String> it = TOKENS.iterator();
            while (it.hasNext() && needDeletedNum-- > 0) {
                it.remove();
            }
            return true;
        }
    }

    public static <T> Collector registerMetricsCollector(Class<T> clazz, final String name, TreeSet<String> labels,
                                                         final String help) throws Exception {
        Collector collector = COLLECTOR_MAP.get(name);
        if (collector == null) {
            synchronized (MetricsUtils.class) {
                try {
                    Collector oldCollector = COLLECTOR_MAP.get(name);
                    if (oldCollector != null) {
                        return oldCollector;
                    }
                    if (clazz == Counter.class) {
                        collector =
                                Counter.build().labelNames(labels2StringArray(labels)).name(name).help(help).register(REGISTRY).register();
                    } else if (clazz == Gauge.class) {
                        collector =
                                Gauge.build().labelNames(labels2StringArray(labels)).name(name).help(help).register(REGISTRY).register();
                    } else if (clazz == Histogram.class) {
                        collector = Histogram.build().buckets(BUCKETS).labelNames(labels2StringArray(labels)).name(name)
                            .help(help).register(REGISTRY).register();
                    } else if (clazz == Summary.class) {
                        collector =
                                Summary.build().labelNames(labels2StringArray(labels)).name(name).help(help).register(REGISTRY).register();
                    } else {
                        throw new Exception("Wrong metrics type: " + clazz.getName());
                    }
                } catch (Exception e) {
                    throw new Exception(String.format("Register metrics failed, metrics name: %s, metrics type: %s",
                            name, clazz.getName()));
                }
                COLLECTOR_MAP.putIfAbsent(name, collector);
                return collector;
            }
        }
        return collector;
    }

    public static Counter.Child getCounter(final String name, TreeMap<String, String> labelValues,
                                           final String help) throws Exception {
        Collector collector = registerMetricsCollector(Counter.class, name,
                new TreeSet<String>(labelValues.keySet()), help);
        if (!(collector instanceof Counter)) {
            throw new Exception("Metrics type not match, needed type: Counter");
        }
        Counter counter = (Counter) collector;
        return counter.labels(values2StringArray(new ArrayList<String>(labelValues.values())));
    }

    public static Gauge.Child getGauge(final String name, TreeMap<String, String> labelValues,
                                       final String help) throws Exception {
        Collector collector = registerMetricsCollector(Gauge.class, name,
                new TreeSet<String>(labelValues.keySet()), help);
        if (!(collector instanceof Gauge)) {
            throw new Exception("Metrics type not match, needed type: Gauge");
        }
        Gauge gauge = (Gauge) collector;
        return gauge.labels(values2StringArray(new ArrayList<String>(labelValues.values())));
    }

    public static Summary.Child getSummary(final String name, TreeMap<String, String> labelValues,
                                           final String help) throws Exception {
        Collector collector = registerMetricsCollector(Summary.class, name, new TreeSet<String>(labelValues.keySet())
                , help);
        if (!(collector instanceof Summary)) {
            throw new Exception("Metrics type not match, needed type: Summary");
        }
        Summary summary = (Summary) collector;
        return summary.labels(values2StringArray(new ArrayList<String>(labelValues.values())));
    }

    public static Histogram.Child getHistogram(final String name, TreeMap<String, String> labelValues,
                                               final String help) throws Exception {
        Collector collector = registerMetricsCollector(Histogram.class, name,
                new TreeSet<String>(labelValues.keySet()), help);
        if (!(collector instanceof Histogram)) {
            throw new Exception("Metrics type not match, needed type: Histogram");
        }
        Histogram histogram = (Histogram) collector;
        return histogram.labels(values2StringArray(new ArrayList<String>(labelValues.values())));
    }

    public static String metrics2String() throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        OutputStreamWriter osw = new OutputStreamWriter(stream);
        TextFormat.write004(osw, REGISTRY.filteredMetricFamilySamples(new HashSet<String>()));
        osw.flush();
        osw.close();
        return new String(stream.toByteArray());
    }
}
