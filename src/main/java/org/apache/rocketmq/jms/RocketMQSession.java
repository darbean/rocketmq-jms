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

import java.io.Serializable;
import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import org.apache.rocketmq.jms.ctx.ConnectionContext;
import org.apache.rocketmq.jms.ctx.SessionContext;
import org.apache.rocketmq.jms.msg.RocketMQBytesMessage;
import org.apache.rocketmq.jms.msg.RocketMQObjectMessage;
import org.apache.rocketmq.jms.msg.RocketMQTextMessage;

public class RocketMQSession implements Session {

    private RocketMQConnection connection;

    private int acknowledgeMode;

    private boolean transacted;

    private MessageListener messageListener;

    public RocketMQSession(RocketMQConnection connection, int acknowledgeMode, boolean transacted) {
        this.connection = connection;
        this.acknowledgeMode = acknowledgeMode;
        this.transacted = transacted;
        ConnectionContext.get(connection).addSession(this);
        SessionContext.register(this);
    }

    @Override
    public BytesMessage createBytesMessage() throws JMSException {
        return new RocketMQBytesMessage();
    }

    @Override
    public MapMessage createMapMessage() throws JMSException {
        //todo
        return null;
    }

    @Override
    public Message createMessage() throws JMSException {
        return new RocketMQBytesMessage();
    }

    @Override
    public ObjectMessage createObjectMessage() throws JMSException {
        return new RocketMQObjectMessage();
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable serializable) throws JMSException {
        return new RocketMQObjectMessage(serializable);
    }

    @Override
    public StreamMessage createStreamMessage() throws JMSException {
        throw new UnsupportedOperationException();
    }

    @Override
    public TextMessage createTextMessage() throws JMSException {
        return new RocketMQTextMessage();
    }

    @Override
    public TextMessage createTextMessage(String text) throws JMSException {
        return new RocketMQTextMessage(text);
    }

    @Override
    public boolean getTransacted() throws JMSException {
        return this.transacted;
    }

    @Override
    public int getAcknowledgeMode() throws JMSException {
        return this.acknowledgeMode;
    }

    @Override
    public void commit() throws JMSException {
        //todo
    }

    @Override
    public void rollback() throws JMSException {
        //todo
    }

    @Override
    public void close() throws JMSException {
        //todo
    }

    @Override
    public void recover() throws JMSException {
        //todo
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
    public void run() {
        throw new UnsupportedOperationException();
    }

    @Override
    public MessageProducer createProducer(Destination destination) throws JMSException {
        return new RocketMQProducer(connection, destination);
    }

    @Override
    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        return new RocketMQConsumer(this.connection, this, destination, false);
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException {
        return new RocketMQConsumer(this.connection, this, destination, messageSelector, false);
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector,
        boolean noLocal) throws JMSException {
        // ignore noLocal param as RMQ not support
        return new RocketMQConsumer(this.connection, this, destination, messageSelector, false);
    }

    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) throws JMSException {
        return new RocketMQConsumer(this.connection, this, topic, null, sharedSubscriptionName, false);
    }

    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName,
        String messageSelector) throws JMSException {
        return new RocketMQConsumer(this.connection, this, topic, messageSelector, sharedSubscriptionName, false);
    }

    @Override
    public Queue createQueue(String queueName) throws JMSException {
        return new RocketMQQueue(queueName);
    }

    @Override
    public Topic createTopic(String topicName) throws JMSException {
        return new RocketMQTopic(topicName);
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException {
        return new RocketMQTopicSubscriber(this.connection, topic, null, name, true);
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name, String messageSelector,
        boolean noLocal) throws JMSException {
        return new RocketMQTopicSubscriber(this.connection, topic, messageSelector, name, true);
    }

    @Override
    public MessageConsumer createDurableConsumer(Topic topic, String name) throws JMSException {
        return new RocketMQConsumer(this.connection, this, topic, null, name, true);
    }

    @Override
    public MessageConsumer createDurableConsumer(Topic topic, String name, String messageSelector,
        boolean noLocal) throws JMSException {
        return new RocketMQConsumer(this.connection, this, topic, messageSelector, name, true);
    }

    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name) throws JMSException {
        return new RocketMQConsumer(this.connection, this, topic, null, name, true);
    }

    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name,
        String messageSelector) throws JMSException {
        return new RocketMQConsumer(this.connection, this, topic, messageSelector, name, true);
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        //todo
        return null;
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException {
        //todo
        return null;
    }

    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        //todo
        return null;
    }

    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        //todo
        return null;
    }

    @Override
    public void unsubscribe(String name) throws JMSException {
        //todo
    }
}
