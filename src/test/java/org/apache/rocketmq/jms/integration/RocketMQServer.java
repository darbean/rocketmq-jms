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

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.BrokerConfig;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.namesrv.NamesrvConfig;
import com.alibaba.rocketmq.namesrv.NamesrvController;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.netty.NettyServerConfig;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import static java.io.File.separator;
import static java.lang.String.format;
import static org.apache.commons.lang.exception.ExceptionUtils.getStackTrace;

public class RocketMQServer {
    public static Logger logger = LoggerFactory.getLogger(RocketMQServer.class);
    private static RocketMQServer server = new RocketMQServer();
    private static final SimpleDateFormat sf = new SimpleDateFormat("yyyyMMddHHmmss");
    private static final String rootDir = System.getProperty("user.home") + separator + "rocketmq-jms" + separator;

    private Random random = new Random();
    private String serverDir;
    private volatile boolean started = false;

    //name server
    private String nameServer;
    private NamesrvConfig namesrvConfig = new NamesrvConfig();
    private NettyServerConfig nameServerNettyServerConfig = new NettyServerConfig();
    private NamesrvController namesrvController;

    //broker
    private final String BROKER_NAME = "JmsTestBrokerName";
    private BrokerController brokerController;
    private BrokerConfig brokerConfig = new BrokerConfig();
    private NettyServerConfig nettyServerConfig = new NettyServerConfig();
    private NettyClientConfig nettyClientConfig = new NettyClientConfig();
    private MessageStoreConfig storeConfig = new MessageStoreConfig();

    //MQAdmin client
    private DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();

    private RocketMQServer() {

    }

    public static RocketMQServer instance() {
        return server;
    }

    public synchronized void start() {
        if (started) {
            return;
        }

        createServerDir();

        startNameServer();

        startBroker();

        startMQAdmin();

        addShutdownHook();

        started = true;
    }

    private void createServerDir() {
        for (int i = 0; i < 5; i++) {
            serverDir = rootDir + sf.format(new Date());
            final File file = new File(serverDir);
            if (!file.exists()) {
                return;
            }
        }
        System.out.println("Has retry 5 times to create base dir,but still failed.");
        System.exit(1);
    }

    private void startNameServer() {
        namesrvConfig.setKvConfigPath(serverDir + separator + "namesrv" + separator + "kvConfig.json");
        nameServerNettyServerConfig.setListenPort(9000 + random.nextInt(1000));
        namesrvController = new NamesrvController(namesrvConfig, nameServerNettyServerConfig);
        try {
            Assert.assertTrue(namesrvController.initialize());
            logger.info("Name Server Start:{}", nameServerNettyServerConfig.getListenPort());
            namesrvController.start();
        } catch (Exception e) {
            System.out.println(format("Name Server start failed, stack trace:%s", getStackTrace(e)));
            System.exit(1);
        }
        nameServer = "127.0.0.1:" + nameServerNettyServerConfig.getListenPort();
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, nameServer);
    }

    private void startBroker() {
        brokerConfig.setBrokerName(BROKER_NAME);
        brokerConfig.setBrokerIP1("127.0.0.1");
        brokerConfig.setNamesrvAddr(nameServer);
        storeConfig.setStorePathRootDir(serverDir);
        storeConfig.setStorePathCommitLog(serverDir + separator + "commitlog");
        storeConfig.setHaListenPort(8000 + random.nextInt(1000));
        nettyServerConfig.setListenPort(10000 + random.nextInt(1000));
        brokerController = new BrokerController(brokerConfig, nettyServerConfig, nettyClientConfig, storeConfig);
        defaultMQAdminExt.setNamesrvAddr(nameServer);
        try {
            Assert.assertTrue(brokerController.initialize());
            logger.info("Broker Start name:{} addr:{}", brokerConfig.getBrokerName(), brokerController.getBrokerAddr());
            brokerController.start();

        } catch (Exception e) {
            System.out.println(format("Broker start failed, stack trace:%s", getStackTrace(e)));
            System.exit(1);
        }
    }

    private void startMQAdmin() {
        try {
            defaultMQAdminExt.start();
        } catch (MQClientException e) {
            System.out.println(format("MQAdmin start failed, stack trace:%s", getStackTrace(e)));
            System.exit(1);
        }
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                defaultMQAdminExt.shutdown();
                brokerController.shutdown();
                namesrvController.shutdown();
                deleteFile(new File(rootDir));
            }
        });
    }

    public void deleteFile(File file) {
        if (!file.exists()) {
            return;
        }
        if (file.isFile()) {
            file.delete();
        } else if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (int i = 0; i < files.length; i++) {
                deleteFile(files[i]);
            }
            file.delete();
        }
    }

    public void createTopic(String topic) {
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(topic);
        topicConfig.setReadQueueNums(4);
        topicConfig.setWriteQueueNums(4);
        try {
            defaultMQAdminExt.createAndUpdateTopicConfig(this.brokerController.getBrokerAddr(), topicConfig);
        } catch (Exception e) {
            logger.error("Create topic:{}, addr:{} failed", topic, this.brokerController.getBrokerAddr());
        }
    }

    public void deleteTopic(String topic) {
        try {
            defaultMQAdminExt.deleteTopicInBroker(Sets.newHashSet(this.brokerController.getBrokerAddr()), topic);
        } catch (Exception e) {
            logger.error("Delete topic:{}, addr:{} failed", topic, this.brokerController.getBrokerAddr());
        }
    }

    public String getNameServer() {
        return this.nameServer;
    }
}
