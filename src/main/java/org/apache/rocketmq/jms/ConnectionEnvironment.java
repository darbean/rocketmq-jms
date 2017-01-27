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

import javax.jms.Connection;
import javax.jms.JMSRuntimeException;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectionEnvironment {

    private static ConnectionEnvironment environment = new ConnectionEnvironment();

    private ConcurrentHashMap<Connection, ClientConfig> configMap = new ConcurrentHashMap();

    private ConnectionEnvironment() {

    }

    public static ConnectionEnvironment get() {
        return environment;
    }

    public void putConnectionClientConfig(Connection connection, ClientConfig clientConfig) {
        if (configMap.containsKey(connection)) {
            throw new JMSRuntimeException(String.format("The connection[%s] has been put with ClientConfig[%s]", connection, clientConfig));
        }

        configMap.put(connection, clientConfig);
    }

    public ClientConfig getConnectionClientConfig(Connection connection) {
        if (!configMap.containsKey(connection)) {
            throw new JMSRuntimeException(String.format("There isn't ClientConfig corresponding with connection[%s]", connection));
        }

        return configMap.get(connection);
    }
}
