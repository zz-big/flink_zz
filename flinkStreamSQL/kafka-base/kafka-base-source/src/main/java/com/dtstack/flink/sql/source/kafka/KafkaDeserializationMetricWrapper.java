/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flink.sql.source.kafka;

import com.dtstack.flink.sql.dirtyManager.manager.DirtyDataManager;
import com.dtstack.flink.sql.format.DeserializationMetricWrapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.connectors.kafka.internal.KafkaConsumerThread;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.dtstack.flink.sql.metric.MetricConstant.*;

/**
 * add metric for source
 * <p>
 * company: www.dtstack.com
 *
 * @author: toutian
 * create: 2019/12/24
 */
public class KafkaDeserializationMetricWrapper extends DeserializationMetricWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaDeserializationMetricWrapper.class);

    private AbstractFetcher<Row, ?> fetcher;

    private AtomicBoolean firstMsg = new AtomicBoolean(true);

    private Calculate calculate;

    public KafkaDeserializationMetricWrapper(
            TypeInformation<Row> typeInfo
            , DeserializationSchema<Row> deserializationSchema
            , Calculate calculate
            , DirtyDataManager dirtyDataManager) {
        super(typeInfo, deserializationSchema, dirtyDataManager);
        this.calculate = calculate;
    }

    @Override
    protected void beforeDeserialize() throws IOException {
        super.beforeDeserialize();
        if (firstMsg.compareAndSet(true, false)) {
            try {
                registerPtMetric(fetcher);
            } catch (Exception e) {
                LOG.error("register topic partition metric error.", e);
            }
        }
    }

    protected void registerPtMetric(AbstractFetcher<Row, ?> fetcher) throws Exception {
        Field consumerThreadField = getConsumerThreadField(fetcher);
        consumerThreadField.setAccessible(true);
        KafkaConsumerThread consumerThread = (KafkaConsumerThread) consumerThreadField.get(fetcher);

        Field hasAssignedPartitionsField = consumerThread.getClass().getDeclaredField("hasAssignedPartitions");
        hasAssignedPartitionsField.setAccessible(true);

        //wait until assignedPartitions

        boolean hasAssignedPartitions = (boolean) hasAssignedPartitionsField.get(consumerThread);

        if (!hasAssignedPartitions) {
            throw new RuntimeException("wait 50 secs, but not assignedPartitions");
        }

        Field consumerField = consumerThread.getClass().getDeclaredField("consumer");
        consumerField.setAccessible(true);

        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerField.get(consumerThread);
        Field subscriptionStateField = kafkaConsumer.getClass().getDeclaredField("subscriptions");
        subscriptionStateField.setAccessible(true);

        //topic partitions lag
        SubscriptionState subscriptionState = (SubscriptionState) subscriptionStateField.get(kafkaConsumer);
        Set<TopicPartition> assignedPartitions = subscriptionState.assignedPartitions();

        for (TopicPartition topicPartition : assignedPartitions) {
            MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup(DT_TOPIC_GROUP, topicPartition.topic())
                    .addGroup(DT_PARTITION_GROUP, topicPartition.partition() + "");
            metricGroup.gauge(DT_TOPIC_PARTITION_LAG_GAUGE, (Gauge<Long>) () -> calculate.calc(subscriptionState, topicPartition));
        }
    }

    public void setFetcher(AbstractFetcher<Row, ?> fetcher) {
        this.fetcher = fetcher;
    }

    protected Field getConsumerThreadField(AbstractFetcher fetcher) throws NoSuchFieldException {
        try {
            return fetcher.getClass().getDeclaredField("consumerThread");
        } catch (Exception e) {
            return fetcher.getClass().getSuperclass().getDeclaredField("consumerThread");
        }
    }
}
