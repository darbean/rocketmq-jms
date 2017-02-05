package org.apache.rocketmq.jms.integration;

import org.junit.BeforeClass;

public class IntegrationBaseTest {

    protected static RocketMQServer server;

    protected final String clientId = "coffee";

    @BeforeClass
    public static void beforeClass() {
        server = RocketMQServer.instance();
        server.start();
    }
}
