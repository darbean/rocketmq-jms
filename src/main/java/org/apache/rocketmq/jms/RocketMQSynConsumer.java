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
import com.alibaba.rocketmq.client.exception.MQClientException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RocketMQSynConsumer implements MessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(RocketMQSynConsumer.class);
    private MessageListener messageListener;
    private Connection connection;
    private DefaultMQPullConsumer consumer;
    private ClientConfig clientConfig;
    private Destination destination;
    private String messageSelector;

    protected PullMessageService pullMessageService;

    public RocketMQSynConsumer(Connection connection, Destination destination, boolean durable) {
        this(connection, destination, null, durable);
    }

    public RocketMQSynConsumer(Connection connection, Destination destination, String messageSelector, boolean durable) {
        // default is unshared consume
        this(connection, destination, messageSelector, UUID.randomUUID().toString(), durable);
    }

    public RocketMQSynConsumer(Connection connection, Destination destination, String messageSelector, String sharedSubscriptionName, boolean durable) {
        this.connection = connection;
        this.destination = destination;
        this.clientConfig = ConnectionEnvironment.get().getConnectionClientConfig(connection);
        this.messageSelector = messageSelector == null ? "*" : messageSelector;

        this.consumer = new DefaultMQPullConsumer(sharedSubscriptionName);
        this.consumer.setNamesrvAddr(this.clientConfig.getNamesrvAddr());
        this.consumer.setInstanceName(this.clientConfig.getInstanceName());
        try {
            this.consumer.start();
        } catch (MQClientException e) {
            throw new JMSRuntimeException("Fail to start syn consumer, error msg:%s", ExceptionUtils.getStackTrace(e));
        }

        this.pullMessageService = new PullMessageService(this.consumer, destination, messageSelector, durable);
        this.pullMessageService.start();
    }


    @Override
    public String getMessageSelector() throws JMSException {
        return messageSelector;
    }

    @Override
    public MessageListener getMessageListener() throws JMSException {
        return this.messageListener;
    }

    @Override
    public void setMessageListener(MessageListener listener) throws JMSException {
        this.messageListener = listener;
    }

    @Override
    public Message receive() throws JMSException {
        return this.pullMessageService.poll();
    }

    @Override
    public Message receive(long timeout) throws JMSException {
        return this.pullMessageService.poll(timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public Message receiveNoWait() throws JMSException {
        return receive(1);
    }

    @Override
    public void close() throws JMSException {
        this.pullMessageService.shutdown();
        this.consumer.shutdown();
    }

}
