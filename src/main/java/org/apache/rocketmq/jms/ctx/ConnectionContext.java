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

package org.apache.rocketmq.jms.ctx;

import com.alibaba.rocketmq.client.ClientConfig;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.jms.Connection;
import org.apache.rocketmq.jms.RocketMQConsumer;
import org.apache.rocketmq.jms.RocketMQProducer;
import org.apache.rocketmq.jms.RocketMQSession;

public class ConnectionContext {

    private static ConcurrentHashMap<Connection, ConnectionContext> contexts = new ConcurrentHashMap();

    private Connection connection;

    private ClientConfig clientConfig;

    private Set<RocketMQSession> sessions = new HashSet();

    private Set<RocketMQProducer> producers = new HashSet();

    private Set<RocketMQConsumer> consumers = new HashSet();

    private ConnectionContext(Connection connection) {
        this.connection = connection;
    }

    public static ConnectionContext get(Connection connection) {
        return contexts.get(connection);
    }

    public static ConnectionContext register(Connection connection) {
        final ConnectionContext context = new ConnectionContext(connection);
        contexts.put(connection, context);
        return context;
    }

    public ClientConfig getClientConfig() {
        return clientConfig;
    }

    public void setClientConfig(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    public void addSession(RocketMQSession session) {
        this.sessions.add(session);
    }

    public Set<RocketMQSession> getSessions() {
        return this.sessions;
    }

    public void addProducer(RocketMQProducer producer) {
        this.producers.add(producer);
    }

    public Set<RocketMQProducer> getProducers() {
        return this.producers;
    }

    public void addConsumer(RocketMQConsumer consumer) {
        this.consumers.add(consumer);
    }

    public Set<RocketMQConsumer> getConsumers() {
        return this.consumers;
    }
}
