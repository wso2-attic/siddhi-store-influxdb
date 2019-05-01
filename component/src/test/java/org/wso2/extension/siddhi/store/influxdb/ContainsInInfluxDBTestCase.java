/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.extension.siddhi.store.influxdb;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.influxdb.InfluxDBException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.DATABASE;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.PASSWORD;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.URL;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.USERNAME;

public class ContainsInInfluxDBTestCase {

    private static final Log log = LogFactory.getLog(ContainsInInfluxDBTestCase.class);
    private AtomicInteger eventCount = new AtomicInteger(0);
    private int inEventCount;
    private int waitTime = 500;
    private int timeout = 30000;
    private boolean eventArrived;

    @BeforeClass
    public static void startTest() {
        log.info("==InfluxDB Table contains tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== InfluxDB Table contains tests completed ==");
    }

    @BeforeMethod
    public void init() {
        try {
            inEventCount = 0;
            eventArrived = false;
            InfluxDBTestUtils.initDatabaseTable(TABLE_NAME);
        } catch (InfluxDBException e) {
            log.info("Test case inored due to :" + e.getMessage());
        }
    }

    @Test(description = "Test contains with one condition.")
    public void containsCheckTestWithSingleCondition() throws InterruptedException, InfluxDBException {
        log.info("containsCheckTestWithSingleCondition");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (name string,value long);\n" +
                "define stream StockStream (symbol string, price float, volume long,time long);\n" +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", influxdb.database = \"" + DATABASE
                + "\")\n" +
                "@Index(\"symbol\",\"time\")" +
                "define table StockTable (symbol string, price float, volume long,time long);\n";
        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "select *\n" +
                "insert into StockTable;\n" +
                "@info(name = 'query2')\n " +
                "from FooStream \n" +
                "[(StockTable.symbol ==name ) in StockTable]\n" +
                "insert into OutputStream;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long l, Event[] events, Event[] events1) {

                EventPrinter.print(l, events, events1);
                if (events != null) {
                    eventArrived = true;
                    inEventCount++;
                    for (Event event : events) {
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(new Object[]{"WSO2", 100}, event.getData());
                                break;
                            case 2:
                                Assert.assertEquals(new Object[]{"IBM", 10}, event.getData());
                                break;
                        }
                    }
                } else {
                    eventArrived = false;
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L, 1548181800007L});
        stockStream.send(new Object[]{"IBM", 65.6f, 10L, 1548181800500L});
        stockStream.send(new Object[]{"CSC", 65.6f, 10L, 1548181800800L});
        fooStream.send(new Object[]{"WSO2", 100});
        fooStream.send(new Object[]{"IBM", 10});
        fooStream.send(new Object[]{"WSO22", 100});
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        Assert.assertEquals(inEventCount, 2, "Number of success events");
        Assert.assertEquals(eventArrived, true, "success");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Test contains with two conditions.")
    public void containsCheckTestWithTwoConditions() throws InterruptedException, InfluxDBException {
        log.info("containsCheckTestWithTwoConditions");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (name string,value long);\n" +
                "define stream StockStream (symbol string, price float, volume long,time long);\n" +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", influxdb.database = \"" + DATABASE
                + "\")\n" +
                "@Index(\"symbol\",\"time\")" +
                "define table StockTable (symbol string, price float, volume long,time long);\n";
        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "select *\n" +
                "insert into StockTable;\n" +
                "@info(name = 'query2')\n " +
                "from FooStream \n" +
                "[(StockTable.symbol == name and StockTable.volume == value) in StockTable]\n" +
                "insert into OutputStream;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long l, Event[] events, Event[] events1) {
                EventPrinter.print(l, events, events1);
                if (events != null) {
                    eventArrived = true;
                    inEventCount++;
                    for (Event event : events) {
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(new Object[]{"WSO2", 100}, event.getData());
                                break;
                            case 2:
                                Assert.assertEquals(new Object[]{"IBM", 101}, event.getData());
                                break;
                        }
                    }
                } else {
                    eventArrived = false;
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L, 1548181800007L});
        stockStream.send(new Object[]{"IBM", 65.6f, 10L, 1548181800500L});
        stockStream.send(new Object[]{"CSC", 65.6f, 10L, 1548181800800L});
        fooStream.send(new Object[]{"WSO2", 100});
        fooStream.send(new Object[]{"IBM", 101});
        fooStream.send(new Object[]{"WSO22", 100});
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        Assert.assertEquals(inEventCount, 1, "Number of success events");
        Assert.assertEquals(eventArrived, true, "success");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing with already defined outputstream.")
    public void containsCheckTestWithDefinedStream() throws InterruptedException, InfluxDBException {
        log.info("containsCheckTestWithDefinedStream");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (name string,value long);\n" +
                "@sink(type='log')" +
                "define stream OutputStream (name string,value long);" +
                "define stream StockStream (symbol string, price float, volume long,time long);\n" +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", influxdb.database = \"" + DATABASE
                + "\")\n" +
                "@Index(\"symbol\")" +
                "define table StockTable(symbol string, price float, volume long,time long);\n";
        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "select *\n" +
                "insert into StockTable;\n" +
                "@info(name = 'query2')\n " +
                "from FooStream \n" +
                "[(StockTable.symbol ==name ) in StockTable]\n" +
                "insert into OutputStream;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long l, Event[] events, Event[] events1) {
                EventPrinter.print(l, events, events1);
                if (events != null) {
                    eventArrived = true;
                    inEventCount++;
                    for (Event event : events) {
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(new Object[]{"WSO2", 100}, event.getData());
                                break;
                            case 2:
                                Assert.assertEquals(new Object[]{"IBM", 10}, event.getData());
                                break;
                        }
                    }
                } else {
                    eventArrived = false;
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L, 1548181800007L});
        stockStream.send(new Object[]{"IBM", 65.6f, 10L, 1548181800500L});
        stockStream.send(new Object[]{"CSC", 65.6f, 10L, 1548181800800L});
        fooStream.send(new Object[]{"WSO2", 100});
        fooStream.send(new Object[]{"IBM", 10});
        fooStream.send(new Object[]{"WSO22", 100});
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        Assert.assertEquals(inEventCount, 2, "Number of success events");
        Assert.assertEquals(eventArrived, true, "success");
        siddhiAppRuntime.shutdown();
    }
}
