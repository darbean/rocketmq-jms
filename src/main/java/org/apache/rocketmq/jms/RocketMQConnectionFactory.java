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
import com.alibaba.rocketmq.client.impl.MQClientManager;
import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import java.util.UUID;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import org.apache.rocketmq.jms.ctx.ConnectionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocketMQConnectionFactory implements ConnectionFactory {

    private static Logger logger = LoggerFactory.getLogger(RocketMQConnectionFactory.class);

    private String namesrvAddr;

    private String clientId;

    public RocketMQConnectionFactory(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public RocketMQConnectionFactory(String namesrvAddr, String clientId) {
        this.namesrvAddr = namesrvAddr;
        this.clientId = clientId;
    }

    @Override
    public Connection createConnection() throws JMSException {
        return createConnection(null, null);
    }

    /**
     * Using userName and Password to create a connection. Now access RMQ broker
     * is anonymous and any userName/password is legal.
     *
     * @param userName ignored
     * @param password ignored
     * @return the new JMS Connection
     * @throws JMSException
     */
    @Override
    public Connection createConnection(String userName, String password) throws JMSException {
        return createRocketMQConnection(userName, password);
    }

    private Connection createRocketMQConnection(String userName, String password) throws JMSException {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setNamesrvAddr(namesrvAddr);
        clientConfig.setInstanceName(UUID.randomUUID().toString());

        MQClientInstance mqClientInstance = MQClientManager.getInstance().getAndCreateMQClientInstance(clientConfig);
        RocketMQConnection connection = new RocketMQConnection(mqClientInstance);
        connection.setClientID(clientId);

        ConnectionContext context = ConnectionContext.register(connection);
        context.setClientConfig(clientConfig);

        return connection;
    }

    @Override
    public JMSContext createContext() {
        //todo:
        return null;
    }

    @Override
    public JMSContext createContext(String userName, String password) {
        //todo:
        return null;
    }

    @Override
    public JMSContext createContext(String userName, String password, int sessionMode) {
        //todo:
        return null;
    }

    @Override
    public JMSContext createContext(int sessionMode) {
        //todo:
        return null;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }
}
