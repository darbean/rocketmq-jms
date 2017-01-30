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

package org.apache.rocketmq.jms.integration;

import java.util.UUID;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import org.apache.rocketmq.jms.RocketMQConnectionFactory;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.notNullValue;

public class JmsClientTest extends IntegrationBaseTest {

    /**
     * Normal test: send and receive a durable message
     *
     * @throws Exception
     */
    @Test
    public void testProduceAndConsume() throws Exception {
        final String rmqTopicName = "coffee-" + UUID.randomUUID().toString();
        server.createTopic(rmqTopicName);

        ConnectionFactory factory = new RocketMQConnectionFactory(server.getNameServer(), clientId);
        Connection connection = factory.createConnection();
        Session session = connection.createSession();
        connection.start();
        Topic topic = session.createTopic(rmqTopicName);

        try {
            //producer
            TextMessage message = session.createTextMessage("mocha coffee,please");
            MessageProducer producer = session.createProducer(topic);
            producer.send(message);

            //consumer
            MessageConsumer consumer = session.createDurableConsumer(topic, "consumer");

            connection.start();

            Message msg = consumer.receive();

            assertThat(msg, notNullValue());

            connection.close();
        }
        finally {
            server.deleteTopic(rmqTopicName);
        }

    }

}