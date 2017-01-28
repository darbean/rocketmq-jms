package org.apache.rocketmq.jms.integration;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import org.apache.rocketmq.jms.RocketMQConnectionFactory;
import org.apache.rocketmq.jms.RocketMQTopic;
import org.junit.Test;

public class SessionTest extends IntegrationBaseTest {

    @Test
    public void testProducerSendOrderly() throws Exception {
        ConnectionFactory factory = new RocketMQConnectionFactory(server.getNameServer(), clientId);
        Connection connection = factory.createConnection();
        Session session = connection.createSession();
        connection.start();

        Destination top1 = new RocketMQTopic("test-session-single-1");
        Destination top2 = new RocketMQTopic("test-session-single-2");
        MessageProducer producer1 = session.createProducer(top1);
        MessageProducer producer2 = session.createProducer(top2);

    }
}
