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

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;
import org.apache.commons.lang.exception.ExceptionUtils;

import static com.google.common.base.Preconditions.checkArgument;
import static javax.jms.Session.SESSION_TRANSACTED;

public class RocketMQConnection implements Connection {
    private final AtomicBoolean started = new AtomicBoolean(false);
    protected String clientID;
    protected ExceptionListener exceptionListener;

    private MQClientInstance clientInstance;

    public RocketMQConnection(MQClientInstance clientInstance) {
        this.clientInstance = clientInstance;
    }

    @Override
    public Session createSession(int sessionMode) throws JMSException {
        if (sessionMode == SESSION_TRANSACTED) {
            return createSession(true, Session.AUTO_ACKNOWLEDGE);
        } else {
            return createSession(false, sessionMode);
        }
    }

    @Override
    public Session createSession() throws JMSException {
        return createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
    }

    @Override
    public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
        checkArgs(transacted, acknowledgeMode);

        return new RocketMQSession(this, acknowledgeMode, transacted);
    }

    private void checkArgs(boolean transacted, int acknowledgeMode) {
        //todo: support local transaction
        checkArgument(!transacted, "Not support local transaction Session at present");

        //todo: support other acknowledgeMode
        checkArgument(Session.DUPS_OK_ACKNOWLEDGE == acknowledgeMode,
                "Only support DUPS_OK_ACKNOWLEDGE now");
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Destination destination,
                                                       String messageSelector,
                                                       ServerSessionPool sessionPool,
                                                       int maxMessages) throws JMSException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName,
                                                              String messageSelector,
                                                              ServerSessionPool sessionPool,
                                                              int maxMessages) throws JMSException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectionConsumer createSharedConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectionConsumer createSharedDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getClientID() throws JMSException {
        return this.clientID;
    }

    @Override
    public void setClientID(String clientID) throws JMSException {
        this.clientID = clientID;
    }

    @Override
    public ConnectionMetaData getMetaData() throws JMSException {
        return RocketMQConnectionMetaData.instance();
    }

    @Override
    public ExceptionListener getExceptionListener() throws JMSException {
        return this.exceptionListener;
    }

    @Override
    public void setExceptionListener(ExceptionListener listener) throws JMSException {
        this.exceptionListener = listener;
    }

    @Override
    public void start() throws JMSException {
        if (started.compareAndSet(false, true)) {
            try {
                this.clientInstance.start();
            } catch (MQClientException e) {
                throw new JMSException(String.format("Fail to start connection:%s",
                        ExceptionUtils.getStackTrace(e)), ErrorCodes.CONNECTION_START_FAILED
                );
            }
        }
    }

    @Override
    public void stop() throws JMSException {
        //todo: Add constrain that must stop before adding new consumer
    }

    @Override
    public void close() throws JMSException {
        if (started.compareAndSet(true, false)) {
            this.clientInstance.shutdown();
        }
    }

    /**
     * Whether the connection is started.
     *
     * @return whether the connection is started.
     */
    public boolean isStarted() {
        return started.get();
    }

    public MQClientInstance getClientInstance() {
        return clientInstance;
    }
}
