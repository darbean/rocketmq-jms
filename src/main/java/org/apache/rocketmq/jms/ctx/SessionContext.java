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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.jms.Session;
import org.apache.rocketmq.jms.RocketMQConsumer;
import org.apache.rocketmq.jms.RocketMQProducer;

public class SessionContext {

    private static ConcurrentMap<Session, SessionContext> contexts = new ConcurrentHashMap();

    private Session session;

    private List<RocketMQConsumer> asyncConsumers = new ArrayList();

    private List<RocketMQConsumer> syncConsumers = new ArrayList();

    private List<RocketMQProducer> producers = new ArrayList();

    public static SessionContext get(Session session) {
        return contexts.get(session);
    }

    public static SessionContext register(Session session) {
        final SessionContext context = new SessionContext(session);
        contexts.put(session, context);
        return context;
    }

    private SessionContext(Session session) {
        this.session = session;
    }

    public void addSyncConsumer(RocketMQConsumer consumer) {
        this.syncConsumers.add(consumer);
    }

    public void addAsyncConsumer(RocketMQConsumer consumer) {
        this.asyncConsumers.add(consumer);
    }

    public void addProducer(RocketMQProducer producer) {
        this.producers.add(producer);
    }

    public boolean isAsyncModel() {
        return !asyncConsumers.isEmpty();
    }

    public boolean isSyncModel() {
        return !syncConsumers.isEmpty();
    }
}
