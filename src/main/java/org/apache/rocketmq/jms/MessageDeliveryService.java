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

import com.alibaba.rocketmq.client.ClientConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.ServiceThread;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.rocketmq.jms.support.JmsHelper;
import org.apache.rocketmq.jms.support.MessageConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service deliver messages synchronous or asynchronous, refer to {@link #handleMessage(MessageExt)}.
 */
public class MessageDeliveryService extends ServiceThread {

    private static final Logger log = LoggerFactory.getLogger(MessageDeliveryService.class);
    private static final AtomicLong COUNTER = new AtomicLong(0L);
    private static final int PULL_BATCH_SIZE = 100;

    private RocketMQConsumer consumer;
    private DefaultMQPullConsumer rocketMQPullConsumer;
    private Destination destination;
    private String consumerGroup;
    private String topicName;

    /** only support RMQ subExpression */
    private String messageSelector;

    /**
     * If durable is true, consume message from the offset consumed last time.
     * Otherwise consume from the max offset
     */
    private boolean durable = false;

    private BlockingQueue<MessageExt> msgQueue = new ArrayBlockingQueue(PULL_BATCH_SIZE);
    private volatile boolean pause = true;

    private MessageListener messageListener;

    public MessageDeliveryService(RocketMQConsumer consumer, Destination destination, String consumerGroup) {
        this.consumer = consumer;
        this.destination = destination;
        this.consumerGroup = consumerGroup;

        createAndStartRocketMQPullConsumer();

        this.topicName = JmsHelper.getTopicName(destination);
    }

    private void createAndStartRocketMQPullConsumer() {
        final ClientConfig clientConfig = this.consumer.getSession().getConnection().getClientConfig();
        this.rocketMQPullConsumer = new DefaultMQPullConsumer(consumerGroup);
        this.rocketMQPullConsumer.setNamesrvAddr(clientConfig.getNamesrvAddr());
        this.rocketMQPullConsumer.setInstanceName(clientConfig.getInstanceName());

        try {
            this.rocketMQPullConsumer.start();
        }
        catch (MQClientException e) {
            throw new JMSRuntimeException("Fail to start RocketMQ pull consumer, error msg:%s", ExceptionUtils.getStackTrace(e));
        }
    }

    @Override
    public String getServiceName() {
        return MessageDeliveryService.class.getSimpleName() + "-" + COUNTER.incrementAndGet();
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!isStoped()) {
            if (pause) {
                this.waitForRunning(1000);
                continue;
            }

            try {
                pullMessage();
            }
            catch (Exception e) {
                log.error("Error during pulling messages", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    public MessageListener getMessageListener() {
        return messageListener;
    }

    public void setMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
    }

    private void pullMessage() throws Exception {
        Set<MessageQueue> mqs = this.rocketMQPullConsumer.fetchSubscribeMessageQueues(this.topicName);
        for (MessageQueue mq : mqs) {
            Long offset = beginOffset(mq);
            PullResult pullResult = this.rocketMQPullConsumer.pull(mq, this.messageSelector, offset, PULL_BATCH_SIZE);

            switch (pullResult.getPullStatus()) {
                case FOUND:
                    List<MessageExt> msgs = pullResult.getMsgFoundList();
                    for (MessageExt msg : msgs) {
                        handleMessage(msg);
                    }
                    //todo: if process crash before consumer receive messages stored in this queue, these messages will be lost
                    this.rocketMQPullConsumer.updateConsumeOffset(mq, pullResult.getMaxOffset());
                    log.info("Pull {} messages from topic:{},broker:{},queueId:{}", msgs.size(), mq.getTopic(), mq.getBrokerName(), mq.getQueueId());
                    break;
                case NO_NEW_MSG:
                case NO_MATCHED_MSG:
                    break;
                case OFFSET_ILLEGAL:
                    throw new JMSException("Error during pull message[reason:OFFSET_ILLEGAL]");
            }
        }
    }

    /**
     * Refer to {@link #durable}.
     *
     * @param mq message queue
     * @return offset
     * @throws MQClientException
     */
    private Long beginOffset(MessageQueue mq) throws MQClientException {
        return this.durable ? this.rocketMQPullConsumer.fetchConsumeOffset(mq, false) : this.rocketMQPullConsumer.maxOffset(mq);
    }

    /**
     * If {@link #messageListener} is set, messages pulled from broker are handled by the {@link
     * MessageListener#onMessage(Message)} in this thread. In other words, relative to the consumer created thread,
     * messages are handled asynchronous.
     *
     * If {@link #messageListener} is not set, messages pulled from broker are put
     * into a memory blocking queue, waiting for the {@link MessageConsumer#receive()}
     * using {@link BlockingQueue#poll()} to handle messages synchronous.
     *
     * @param msg to handle message
     * @throws InterruptedException
     * @throws JMSException
     */
    private void handleMessage(MessageExt msg) throws InterruptedException, JMSException {
        if (this.messageListener == null) {
            this.msgQueue.put(msg);
        }
        else {
            this.messageListener.onMessage(MessageConverter.convert2JMSMessage(msg));
        }
    }

    public Message poll() throws JMSException {
        try {
            MessageExt msgExt = this.msgQueue.take();
            return MessageConverter.convert2JMSMessage(msgExt);
        }
        catch (InterruptedException e) {
            throw new JMSException(e.getMessage());
        }
    }

    public Message poll(long timeout, TimeUnit timeUnit) throws JMSException {
        try {
            MessageExt msgExt = this.msgQueue.poll(timeout, timeUnit);
            return MessageConverter.convert2JMSMessage(msgExt);
        }
        catch (InterruptedException e) {
            throw new JMSException(e.getMessage());
        }
    }

    public void pause() {
        this.pause = true;
    }

    public void recover() {
        this.pause = false;
    }

    public void close() {
        log.info("Begin to close message delivery service:{}", toString());

        this.stop();

        // waiting for pending messages being received by consumer to prevent these messages from missing
        while (!msgQueue.isEmpty()) {
            this.waitForRunning(1000);
        }

        this.rocketMQPullConsumer.shutdown();

        this.shutdown();

        log.info("Success to close message delivery service:{}", toString());
    }

    public void setMessageSelector(String messageSelector) {
        this.messageSelector = messageSelector;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

}
