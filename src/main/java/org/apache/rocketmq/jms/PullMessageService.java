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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.jms;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.jms.support.JmsHelper;
import org.apache.rocketmq.jms.support.MessageConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class PullMessageService extends Thread {

    private static final Logger log = LoggerFactory.getLogger(PullMessageService.class);
    private static final AtomicLong COUNTER = new AtomicLong(0L);
    private DefaultMQPullConsumer consumer;
    private BlockingQueue<MessageExt> msgQueue = new ArrayBlockingQueue(1000 * 20);
    private Destination destination;
    private String topicName;
    /** todo: only support RMQ subExpression right now */
    private String messageSelector;
    /** If durable is true, consume message from the offset consumed last time. Otherwise consume from the max offset */
    private boolean durable;
    private volatile boolean stopped = false;

    public PullMessageService(DefaultMQPullConsumer consumer, Destination destination, String messageSelector, boolean durable) {
        this.consumer = consumer;
        this.destination = destination;
        this.topicName = JmsHelper.getTopicName(destination);
        this.messageSelector = messageSelector;
        this.durable = durable;
        this.setName(PullMessageService.class.getSimpleName() + "-" + COUNTER.incrementAndGet());
    }

    @Override
    public void run() {
        while (!stopped) {
            try {
                Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(topicName);
                for (MessageQueue mq : mqs) {
                    Long offset = durable ? consumer.fetchConsumeOffset(mq, false) : consumer.maxOffset(mq);
                    PullResult pullResult = consumer.pullBlockIfNotFound(mq, messageSelector, offset, 100);
                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            List<MessageExt> msgs = pullResult.getMsgFoundList();
                            for (MessageExt msg : msgs) {
                                msgQueue.put(msg);
                            }
                            consumer.updateConsumeOffset(mq, pullResult.getMaxOffset());
                            break;
                        case NO_NEW_MSG:
                        case NO_MATCHED_MSG:
                            break;
                        case OFFSET_ILLEGAL:
                            throw new JMSException("Error during pull message[reason:OFFSET_ILLEGAL]");
                    }
                }
                Thread.sleep(100);
            } catch (Exception e) {
                log.error("Error during pull message.", e);
            }
        }
    }

    public Message poll() throws JMSException {
        try {
            MessageExt msgExt = this.msgQueue.take();
            return MessageConverter.convert2JMSMessage(msgExt);
        } catch (InterruptedException e) {
            throw new JMSException(e.getMessage());
        }
    }

    public Message poll(long timeout, TimeUnit timeUnit) throws JMSException {
        try {
            MessageExt msgExt = this.msgQueue.poll(timeout, timeUnit);
            return MessageConverter.convert2JMSMessage(msgExt);
        } catch (InterruptedException e) {
            throw new JMSException(e.getMessage());
        }
    }

    public void shutdown() {
        this.stopped = true;
    }
}
