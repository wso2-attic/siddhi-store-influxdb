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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.influxdb.InfluxDBException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.SiddhiTestHelper;

import java.util.concurrent.atomic.AtomicInteger;

import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.DATABASE;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.PASSWORD;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.URL;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.USERNAME;

public class ContainsInInfluxDBTestCase {

    private static final Log log = LogFactory.getLog(ContainsInInfluxDBTestCase.class);

    private AtomicInteger eventCount = new AtomicInteger(0);
    private int waitTime = 50;
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
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
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

        Thread.sleep(1000);
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
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
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

        Thread.sleep(500);
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
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
                + "\")\n" +
                "@Index(\"symbol\")" +
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

        Thread.sleep(500);
        Assert.assertEquals(eventArrived, true, "success");
        siddhiAppRuntime.shutdown();
    }
}
