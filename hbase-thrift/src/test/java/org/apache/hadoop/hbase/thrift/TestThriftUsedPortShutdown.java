/*
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.thrift;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.IncrementingEnvironmentEdge;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.experimental.categories.Category;

import org.eclipse.jetty.server.Server;

import org.apache.hadoop.hbase.shaded.com.google.common.base.Joiner;

/**
 * Start the HBase Thrift HTTP server on a random port through the command-line
 * interface and talk to it from client side.
 */
@Category({ClientTests.class, LargeTests.class})

public class TestThriftUsedPortShutdown {

    private static final Log LOG =
            LogFactory.getLog(TestThriftUsedPortShutdown.class);

    private static final HBaseTestingUtility TEST_UTIL =
            new HBaseTestingUtility();

    private Thread thriftServerThread;
    private volatile Exception thriftServerException;

    private ThriftServer thriftServer;
    private int port;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TEST_UTIL.getConfiguration().setBoolean("hbase.regionserver.thrift.http", false);
        TEST_UTIL.getConfiguration().setBoolean("hbase.table.sanity.checks", false);
        TEST_UTIL.getConfiguration().setBoolean("hbase.shutdown.hook", false);
        TEST_UTIL.startMiniCluster();
        //ensure that server time increments every time we do an operation, otherwise
        //successive puts having the same timestamp will override each other
        EnvironmentEdgeManagerTestHelper.injectEdge(new IncrementingEnvironmentEdge());
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        TEST_UTIL.shutdownMiniCluster();
        EnvironmentEdgeManager.reset();
    }

    @Rule
    public volatile ExpectedSystemExit exit = ExpectedSystemExit.none();

    public void useHttpPort(final int port) {
        LOG.info("starting jetty server on port: " + port);

        Thread jettyServerThread = new Thread(new Runnable() {
           @Override
           public void run() {
               try {
                   Server server = new Server(port);
                   server.start();
                   server.join();
               } catch (Exception e) {
                   System.out.println(e.getMessage());
               }
           }
        });
        jettyServerThread.setName("jetty-httpServer");
        jettyServerThread.start();
    }

    private void startThriftServerThread(final String[] args) {
        LOG.info("Starting HBase Thrift server: " + Joiner.on(" ").join(args));

        thriftServerException = null;
        thriftServerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    thriftServer.doMain(args);
                } catch (Exception e) {
                    thriftServerException = e;
                }
            }
        });
        exit.expectSystemExitWithStatus(-1);
        thriftServerThread.setName(ThriftServer.class.getSimpleName() +
                "-server");
        thriftServerThread.start();
    }

    @Test(timeout=600000)
    public void testRunThriftServerWithUsedPort() throws Exception {
        // Test thrift server shuts down if configured port is already in use
        runThriftServer(0);
    }

    private void stopThriftServerThread() throws Exception {
        LOG.debug("Stopping " + " Thrift HTTP server");
        thriftServer.stop();
        thriftServerThread.join();
        if (thriftServerException != null) {
            LOG.error("Command-line invocation of HBase Thrift server threw an " +
                    "exception", thriftServerException);
            throw new Exception(thriftServerException);
        }
    }

    private void runThriftServer(int customHeaderSize) throws Exception {
        List<String> args = new ArrayList<>(3);
        port = HBaseTestingUtility.randomFreePort();
        useHttpPort(port);
        args.add("-" + ThriftServer.PORT_OPTION);
        args.add(String.valueOf(port));
        args.add("start");

        thriftServer = new ThriftServer(TEST_UTIL.getConfiguration());
        try {
            startThriftServerThread(args.toArray(new String[args.size()]));
        } catch (Exception e) {
            LOG.error("error occurred", e);
        }

        // wait up to 10s for the server to start
        for (int i = 0; i < 100
                && ( thriftServer.serverRunner == null ||  thriftServer.serverRunner.httpServer ==
                null); i++) {
            Thread.sleep(100);
        }

        try {
            stopThriftServerThread();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}