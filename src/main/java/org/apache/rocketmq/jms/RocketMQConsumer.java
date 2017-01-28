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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.rocketmq.jms.ctx.ConnectionContext;
import org.apache.rocketmq.jms.ctx.SessionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocketMQConsumer implements MessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(RocketMQConsumer.class);
    private Connection connection;
    private Session session;
    private DefaultMQPullConsumer consumer;
    private ClientConfig clientConfig;
    private Destination destination;
    private String messageSelector;
    private MessageListener messageListener;
    private boolean durable;

    protected PullMessageService pullMessageService;
    private ExecutorService asyncMsgExecutor;

    public RocketMQConsumer(Connection connection, Session session, Destination destination, boolean durable) {
        this(connection, session, destination, null, durable);
    }

    public RocketMQConsumer(Connection connection, Session session, Destination destination, String messageSelector,
        boolean durable) {
        // default is unshared consume
        this(connection, session, destination, messageSelector, UUID.randomUUID().toString(), durable);
    }

    public RocketMQConsumer(Connection connection, Session session, Destination destination, String messageSelector,
        String sharedSubscriptionName, boolean durable) {
        this.connection = connection;
        this.session = session;
        this.destination = destination;
        this.clientConfig = ConnectionContext.get(connection).getClientConfig();
        this.messageSelector = messageSelector == null ? "*" : messageSelector;
        this.durable = durable;

        ConnectionContext.get(connection).addConsumer(this);

        this.consumer = new DefaultMQPullConsumer(sharedSubscriptionName);
        this.consumer.setNamesrvAddr(this.clientConfig.getNamesrvAddr());
        this.consumer.setInstanceName(this.clientConfig.getInstanceName());
        try {
            this.consumer.start();
        }
        catch (MQClientException e) {
            throw new JMSRuntimeException("Fail to start syn consumer, error msg:%s", ExceptionUtils.getStackTrace(e));
        }

        this.pullMessageService = new PullMessageService(this.consumer, destination, messageSelector, durable);
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
        if (SessionContext.get(this.session).isSyncModel()) {
            throw new JMSException("A asynchronously call is not permitted when a session is being used synchronously");
        }

        this.messageListener = listener;

        this.pullMessageService.setMessageListener(listener);
        this.pullMessageService.start();

        SessionContext.get(this.session).addAsyncConsumer(this);
    }

    @Override
    public Message receive() throws JMSException {
        return this.receive(Long.MAX_VALUE);
    }

    @Override
    public Message receive(long timeout) throws JMSException {
        if (SessionContext.get(this.session).isAsyncModel()) {
            throw new JMSException("A synchronous call is not permitted when a session is being used asynchronously.");
        }

        this.pullMessageService.start();

        SessionContext.get(this.session).addSyncConsumer(this);

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

    public void setPause(boolean pause) {
        this.pullMessageService.setPause(pause);
    }

}
