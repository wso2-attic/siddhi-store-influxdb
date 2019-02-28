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
import org.wso2.siddhi.query.compiler.exception.SiddhiParserException;

import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.DATABASE;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.PASSWORD;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.URL;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.USERNAME;

public class ReadEventInfluxDBTestCase {

    private static final Log log = LogFactory.getLog(ReadEventInfluxDBTestCase.class);
    private int removeEventCount;
    private boolean eventArrived;

    @BeforeClass
    public static void startTest() {

        log.info("==InfluxDB Table READ tests started ==");
    }

    @AfterClass
    public static void shutdown() {

        log.info("== InfluxDB Table READ tests completed ==");
    }

    @BeforeMethod
    public void init() {

        try {
            removeEventCount = 0;
            InfluxDBTestUtils.initDatabaseTable(TABLE_NAME);
        } catch (InfluxDBException e) {
            log.info("Test case ignored due to " + e.getMessage());
        }
    }

    @Test(description = "Read events from a InfluxDB table successfully.")
    public void readEventInfluxTableTest() throws InterruptedException, InfluxDBException {

        log.info("readEventsFromInfluxDBTableTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (name string);\n" +
                "define stream StockStream (symbol string, price float, volume long,time long);\n" +
                "define stream OutputStream (checkName string, checkCategory float, checkVolume long,checkTime long);" +
                "\n" +
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
                "from FooStream#window.length(1) join StockTable on StockTable.symbol==FooStream.name \n" +
                "select StockTable.symbol as checkName, StockTable.price as checkCategory, " +
                "StockTable.volume as checkVolume,StockTable.time as checkTime\n" +
                "insert into OutputStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {

                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
            }
        });

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 58.6f, 100L, 1548181800001L});
        stockStream.send(new Object[]{"IBM", 57.6f, 100L, 1548181800009L});
        stockStream.send(new Object[]{"WSO2", 97.6f, 100L, 1548181800007L});
        fooStream.send(new Object[]{"WSO2"});
        fooStream.send(new Object[]{"IBM"});
        fooStream.send(new Object[]{"CSC"});

        Thread.sleep(500);

        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiParserException.class, description = "Read unsuccessfully from non existing table")
    public void readFromNonExistingTableTest() throws InterruptedException, InfluxDBException {

        log.info("readFromNonExistingTableTest");
        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream FooStream (symbol string);";

        String query = "" +
                "@info(name = 'query2')\n " +
                "from FooStream#window.length(2) join StockTable on StockTable.symbol==FooStream.name \n" +
                "select StockTable.symbol as checkName, StockTable.price as checkCategory, " +
                "StockTable.volume as checkVolume,StockTable.time as checkTime\n" +
                "insert into OutputStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();
        fooStream.send(new Object[]{"WSO2"});

        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiParserException.class, description = "Read unsuccessfully without defining stream")
    public void readEventWithoutDefineStreamTest() throws InterruptedException, InfluxDBException {

        log.info("readEventWithoutDefineStreamTest");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long,time long);\n" +
                "define stream OutputStream (checkName string, checkCategory float, checkVolume long,checkTime long);" +
                "\n" +
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
                "from FooStream#window.length(2) join StockTable on StockTable.symbol==FooStream.name \n" +
                "select StockTable.symbol as checkName, StockTable.price as checkCategory, " +
                "StockTable.volume as checkVolume,StockTable.time as checkTime\n" +
                "insert into OutputStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
            }
        });

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 58.6f, 100L, 1548181800001L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L, 1548181800009L});
        stockStream.send(new Object[]{"IBM", 97.6f, 100L, 1548181800007L});
        fooStream.send(new Object[]{"WSO2"});

        Thread.sleep(1000);
        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        siddhiAppRuntime.shutdown();

    }

    @Test(description = "Read  multiple events from a InfluxDB table successfully with window.length.")
    public void readEventsInfluxTableTest() throws InterruptedException, InfluxDBException {

        log.info("readEventsInfluxTableTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (name string);\n" +
                "define stream StockStream (symbol string, price float, volume long,time long);\n" +
                "define stream OutputStream (checkName string, checkCategory float, checkVolume long,checkTime long);" +
                "\n" +
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
                "from FooStream#window.length(1) join StockTable on StockTable.symbol==FooStream.name \n" +
                "select StockTable.symbol as checkName, StockTable.price as checkCategory, " +
                "StockTable.volume as checkVolume,StockTable.time as checkTime\n" +
                "insert into OutputStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
            }
        });

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 58.6f, 110L, 1548181800001L});
        stockStream.send(new Object[]{"IBM", 57.6f, 100L, 1548181800009L});
        stockStream.send(new Object[]{"CSC", 97.6f, 50L, 1548181800707L});
        stockStream.send(new Object[]{"MIT", 97.6f, 10L, 1548181800507L});
        fooStream.send(new Object[]{"WSO2"});
        fooStream.send(new Object[]{"IBM"});
        fooStream.send(new Object[]{"MIT"});
        Thread.sleep(1000);

        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Read  multiple events from a InfluxDB table successfully(window.time).")
    public void readEventsInfluxTableTestCase() throws InterruptedException, InfluxDBException {

        log.info("readEventsFromInfluxDBTableTestCase");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (name string);\n" +
                "define stream StockStream (symbol string, price float, volume long,time long);\n" +
                "define stream OutputStream (checkName string, checkCategory float, checkVolume long,checkTime long);" +
                "\n" +
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
                "from FooStream#window.time(5 sec) join StockTable on StockTable.symbol==FooStream.name \n" +
                "select StockTable.symbol as checkName, StockTable.price as checkCategory, " +
                "StockTable.volume as checkVolume,StockTable.time as checkTime\n" +
                "insert into OutputStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {

                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
            }
        });
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 58.6f, 110L, 1548181800001L});
        stockStream.send(new Object[]{"IBM", 57.6f, 100L, 1548181800009L});
        stockStream.send(new Object[]{"CSC", 97.6f, 50L, 1548181800707L});
        stockStream.send(new Object[]{"MIT", 97.6f, 10L, 1548181800507L});
        fooStream.send(new Object[]{"WSO2"});
        fooStream.send(new Object[]{"IBM"});
        fooStream.send(new Object[]{"MIT"});
        Thread.sleep(1000);

        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing reading multiple events from a influxDB table successfully")
    public void readEventInfluxTableTestCase6() throws InterruptedException, InfluxDBException {

        log.info("readEventsFromInfluxDBTableTestCase6");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (name long);\n" +
                "define stream StockStream (symbol string, price float, volume long,time long);\n" +
                "define stream OutputStream (checkName string, checkPrice float, checkVolume long,checkTime long);" +
                "\n" +
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
                "from FooStream#window.length(1) join StockTable on StockTable.volume==FooStream.name \n" +
                "select StockTable.symbol as checkName, StockTable.price as checkPrice, " +
                "StockTable.volume as checkVolume,StockTable.time as checkTime\n" +
                "group by StockTable.symbol\n" +
                "insert into OutputStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {

                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
            }
        });
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 58.6f, 110L, 1548181800001L});
        stockStream.send(new Object[]{"IBM", 57.6f, 100L, 1548181800009L});
        stockStream.send(new Object[]{"CSC", 97.6f, 50L, 1548181800707L});
        stockStream.send(new Object[]{"MIT", 97.6f, 10L, 1548181800507L});
        fooStream.send(new Object[]{50});
        Thread.sleep(500);

        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing reading  multiple events from a InfluxDB table successfully")
    public void readEventInfluxTableTestCase() throws InterruptedException, InfluxDBException {

        log.info("readEventsFromInfluxDBTableTestCase");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (name string,vol long);\n" +
                "define stream StockStream (symbol string, price float, volume long,time long);\n" +
                "define stream OutputStream (checkName string, checkCategory float, checkVolume long,checkTime long);" +
                "\n" +
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
                "from FooStream#window.time(5 sec) join StockTable on (StockTable.symbol==FooStream.name  \n" +
                "or StockTable.volume+50<FooStream.vol)" +
                "select StockTable.symbol as checkName, StockTable.price as checkCategory, " +
                "StockTable.volume as checkVolume,StockTable.time as checkTime\n" +
                "insert into OutputStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
            }
        });

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 58.6f, 110L, 1548181800001L});
        stockStream.send(new Object[]{"IBM", 57.6f, 100L, 1548181800009L});
        stockStream.send(new Object[]{"CSC", 97.6f, 50L, 1548181800707L});
        stockStream.send(new Object[]{"MIT", 97.6f, 10L, 1548181800507L});
        fooStream.send(new Object[]{"WSO2", 110});
        fooStream.send(new Object[]{"IBM", 50});
        Thread.sleep(1000);

        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Read events from a InfluxDB table successfully without any condition")
    public void readEventsWithoutConditionTest() throws InterruptedException, InfluxDBException {

        log.info("readEventsWithoutConditionTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (name string);\n" +
                "define stream StockStream (symbol string, price float, volume long,time long);\n" +
                "define stream OutputStream (checkName string, checkCategory float, checkVolume long,checkTime long);" +
                "\n" +
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
                "from FooStream#window.length(1) join StockTable  \n" +
                "select StockTable.symbol as checkName, StockTable.price as checkCategory, " +
                "StockTable.volume as checkVolume,StockTable.time as checkTime\n" +
                "insert into OutputStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
            }
        });
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 58.6f, 100L, 1548181800001L});
        stockStream.send(new Object[]{"IBM", 57.6f, 101L, 1548181800009L});
        stockStream.send(new Object[]{"WSO2", 97.6f, 102L, 1548181800007L});
        fooStream.send(new Object[]{"WSO2"});
        Thread.sleep(500);

        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        siddhiAppRuntime.shutdown();
    }
}
