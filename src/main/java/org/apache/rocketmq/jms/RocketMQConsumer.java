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

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocketMQConsumer implements MessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(RocketMQConsumer.class);
    private RocketMQSession session;
    private Destination destination;
    private String messageSelector;
    private MessageListener messageListener;
    private String sharedSubscriptionName;
    private boolean durable;

    private MessageDeliveryService messageDeliveryService;

    public RocketMQConsumer(RocketMQSession session, Destination destination,
        String messageSelector,
        boolean durable) {
        this(session, destination, messageSelector, UUID.randomUUID().toString(), durable);
    }

    public RocketMQConsumer(RocketMQSession session, Destination destination,
        String messageSelector,
        String sharedSubscriptionName, boolean durable) {
        this.session = session;
        this.destination = destination;
        this.messageSelector = messageSelector;
        this.sharedSubscriptionName = sharedSubscriptionName;
        this.durable = durable;

        this.messageDeliveryService = new MessageDeliveryService(this, this.destination, this.sharedSubscriptionName);
        this.messageDeliveryService.setMessageSelector(this.messageSelector);
        this.messageDeliveryService.setDurable(this.durable);
        this.messageDeliveryService.start();
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
        if (this.session.isSyncModel()) {
            throw new JMSException("A asynchronously call is not permitted when a session is being used synchronously");
        }

        this.messageListener = listener;
        this.messageDeliveryService.setConsumeModel(ConsumeModel.ASYNC);
        this.session.addAsyncConsumer(this);
    }

    @Override
    public Message receive() throws JMSException {
        return this.receive(0);
    }

    @Override
    public Message receive(long timeout) throws JMSException {
        if (this.session.isAsyncModel()) {
            throw new JMSException("A synchronous call is not permitted when a session is being used asynchronously.");
        }

        this.session.addSyncConsumer(this);

        if (timeout == 0) {
            MessageWrapper wrapper = this.messageDeliveryService.poll();
            wrapper.getConsumer().getMessageDeliveryService().ack(wrapper.getMq(), wrapper.getOffset());
            return wrapper.getMessage();
        }
        else {
            MessageWrapper wrapper = this.messageDeliveryService.poll(timeout, TimeUnit.MILLISECONDS);
            wrapper.getConsumer().getMessageDeliveryService().ack(wrapper.getMq(), wrapper.getOffset());
            return wrapper.getMessage();
        }
    }

    @Override
    public Message receiveNoWait() throws JMSException {
        return receive(1);
    }

    @Override
    public void close() throws JMSException {
        log.info("Begin to close consumer:{}", toString());

        this.messageDeliveryService.close();

        log.info("Success to close consumer:{}", toString());
    }

    public void start() {
        this.messageDeliveryService.recover();
    }

    public void stop() {
        this.messageDeliveryService.pause();
    }

    public MessageDeliveryService getMessageDeliveryService() {
        return messageDeliveryService;
    }

    public RocketMQSession getSession() {
        return session;
    }
}
