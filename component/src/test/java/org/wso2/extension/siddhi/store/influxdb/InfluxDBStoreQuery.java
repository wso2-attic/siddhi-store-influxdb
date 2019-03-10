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
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.StoreQueryCreationException;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.compiler.SiddhiCompiler;
import org.wso2.siddhi.query.compiler.exception.SiddhiParserException;

import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.DATABASE;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.PASSWORD;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.URL;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.USERNAME;

public class InfluxDBStoreQuery {

    private static final Log log = LogFactory.getLog(InfluxDBStoreQuery.class);

    @BeforeClass
    public static void startTest() {

        log.info("== InfluxDB Table Query tests started ==");
        log.info("");
    }

    @AfterClass
    public static void shutdown() {

        log.info("");
        log.info("==InfluxDB Table Query tests completed ==");
    }

    @BeforeMethod
    public void init() {

        try {
            InfluxDBTestUtils.initDatabaseTable(TABLE_NAME);
        } catch (InfluxDBException e) {
            log.info("Test case ignored due to " + e.getMessage());
        }
    }

    @Test
    public void storeQueryTest1() throws InterruptedException {

        log.info("storeQueryTest1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
                + "\")\n" +
                "@Index(\"symbol\",\"time\")" +
                "define table StockTable (symbol string, price float, volume long,time long); ";
        String query = "" +
                "@info(name='query2') " +
                "from StockStream\n" +
                "select symbol,price,volume,currentTimeMillis() as time\n" +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 59.6f, 100L});
        Thread.sleep(500);

        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on price > 5 AND symbol==\"WSO2\" " +
                "select * " +
                "group by symbol ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(3, events.length);
        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on volume > 10 " +
                "select symbol,volume, time " +
                "limit 2 ");
        EventPrinter.print(events);
        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on price > 5 " +
                "select* " +
                "group by symbol  ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(4, events.length);
        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on volume > 10 " +
                "select symbol, price, time " +
                "offset 1 ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on volume > 10 " +
                "select symbol, volume as totalVolume ,time " +
                "group by symbol " +
                "having symbol == 'WSO2'");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(4, events.length);
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void storeQueryTest2() throws InterruptedException {

        log.info("Test2 table");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
                + "\")\n" +
                "@Index(\"symbol\",\"time\")" +
                "define table StockTable (symbol string, price float, volume long,time long); ";
        String query = "" +
                "@info(name='query2') " +
                "from StockStream\n" +
                "select symbol,price,volume,currentTimeMillis() as time\n" +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(500);

        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable ");

        EventPrinter.print(events);
        AssertJUnit.assertEquals(3, events.length);

        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on price > 75 ");

        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);

        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on price > volume*3/4  ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);

        siddhiAppRuntime.shutdown();

    }

    @Test
    public void storeQueryTest3() throws InterruptedException {

        log.info("Test3 table");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long,time long); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
                + "\")\n" +
                "@Index(\"symbol\",\"time\")" +
                "define table StockTable (symbol string, price float, volume long,time long); ";
        String query = "" +
                "@info(name='query2') " +
                "from StockStream\n" +
                "select symbol,price,volume, time\n" +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L, 1548181800003L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L, 1548181800005L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L, 1548181800005L});
        Thread.sleep(500);

        Event[] events = siddhiAppRuntime.query("" +
//                "from StockTable " +
//                "on volume > 10 " +
//                "select symbol, price, time ");
//        EventPrinter.print(events);
//        AssertJUnit.assertEquals(3, events.length);
//        AssertJUnit.assertEquals(3, events[0].getData().length);
//
//        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "select symbol, volume,time ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(3, events.length);
        AssertJUnit.assertEquals(3, events[0].getData().length);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void storeQueryTest4() throws InterruptedException {

        log.info("Test4 table");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
                + "\")\n" +
                "@Index(\"symbol\",\"time\")" +
                "define table StockTable (symbol string, price float, volume long,time long); ";
        String query = "" +
                "@info(name='query2') " +
                "from StockStream\n" +
                "select symbol,price,volume,currentTimeMillis() as time\n" +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(500);

        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on volume > 10 " +
                "select symbol, price, time " +
                "limit 2 ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(2, events.length);
        AssertJUnit.assertEquals("WSO2", events[0].getData()[1]);
        AssertJUnit.assertEquals("IBM", events[1].getData()[1]);

    }

    @Test(expectedExceptions = StoreQueryCreationException.class)
    public void storeQueryTest5() throws InterruptedException {

        log.info("Test5 table");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
                + "\")\n" +
                "@Index(\"symbol\",\"time\")" +
                "define table StockTable (symbol string, price float, volume long,time long); ";
        String query = "" +
                "@info(name='query2') " +
                "from StockStream\n" +
                "select symbol,price,volume,currentTimeMillis() as time\n" +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        try {
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            siddhiAppRuntime.start();

            Event[] events = siddhiAppRuntime.query("" +
                    "from StockTable " +
                    "on price > 10 " +
                    "select symbol1, volume as totalVolume ,time " +
                    "group by symbol " +
                    "having volume >150");

            EventPrinter.print(events);
            AssertJUnit.assertEquals(1, events.length);
            AssertJUnit.assertEquals(400L, events[0].getData(1));

        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(expectedExceptions = StoreQueryCreationException.class)
    public void storeQueryTest6() throws InterruptedException {

        log.info("Test6 table");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
                + "\")\n" +
                "@Index(\"symbol\",\"time\")" +
                "define table StockTable (symbol string, price float, volume long,time long); ";
        String query = "" +
                "@info(name='query2') " +
                "from StockStream\n" +
                "select symbol,price,volume,currentTimeMillis() as time\n" +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        try {
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            siddhiAppRuntime.start();

            Event[] events = siddhiAppRuntime.query("" +
                    "from StockTable1 " +
                    "on price > 10 " +
                    "select symbol, volume as totalVolume ,time " +
                    "group by symbol " +
                    "having volume >150");

            EventPrinter.print(events);
            AssertJUnit.assertEquals(1, events.length);
            AssertJUnit.assertEquals(400L, events[0].getData(1));

        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(expectedExceptions = SiddhiParserException.class)
    public void storeQueryTest7() {

        log.info("Test7 table");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
                + "\")\n" +
                "@Index(\"symbol\",\"time\")" +
                "define table StockTable (symbol string, price float, volume long,time long); ";
        String query = "" +
                "@info(name='query2') " +
                "from StockStream\n" +
                "select symbol,price,volume,currentTimeMillis() as time\n" +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        try {
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            siddhiAppRuntime.start();

            Event[] events = siddhiAppRuntime.query("" +
                    "from StockTable " +
                    "on price > 10 " +
                    "select symbol, volume as totalVolume ,time" +
                    "group by symbol " +
                    "having volume >150");

            EventPrinter.print(events);
            AssertJUnit.assertEquals(1, events.length);
            AssertJUnit.assertEquals(400L, events[0].getData(1));

        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(expectedExceptions = StoreQueryCreationException.class)
    public void storeQueryTest8() throws InterruptedException {

        log.info("Test9 table");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
                + "\")\n" +
                "@Index(\"symbol\",\"time\")" +
                "define table StockTable (symbol string, price float, volume long,time long); ";
        String query = "" +
                "@info(name='query2') " +
                "from StockStream\n" +
                "select symbol,price,volume,currentTimeMillis() as time\n" +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(500);

        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on symbol == 'IBM' " +
                "select symbol, volume ");
        EventPrinter.print(events);

    }

    @Test
    public void storeQueryTest9() throws InterruptedException {

        log.info("Test9 table");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long,time long); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
                + "\")\n" +
                "@Index(\"symbol\",\"time\")" +
                "define table StockTable (symbol string, price float, volume long,time long); ";
        String storeQuery = "" +
                "from StockTable " +
                "select * ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        Attribute[] actualAttributeArray = siddhiAppRuntime.getStoreQueryOutputAttributes(SiddhiCompiler.parseStoreQuery
                (storeQuery));
        Attribute symbolAttribute = new Attribute("symbol", Attribute.Type.STRING);
        Attribute priceAttribute = new Attribute("price", Attribute.Type.FLOAT);
        Attribute volumeAttribute = new Attribute("volume", Attribute.Type.LONG);
        Attribute timeAttribute = new Attribute("time", Attribute.Type.LONG);
        Attribute[] expectedAttributeArray = new Attribute[]{symbolAttribute, priceAttribute, volumeAttribute,
                timeAttribute};
        AssertJUnit.assertArrayEquals(expectedAttributeArray, actualAttributeArray);

        storeQuery = "" +
                "from StockTable " +
                "select symbol, volume as totalVolume,time ;";
        actualAttributeArray = siddhiAppRuntime.getStoreQueryOutputAttributes(SiddhiCompiler.parseStoreQuery
                (storeQuery));
        Attribute totalVolumeAttribute = new Attribute("totalVolume", Attribute.Type.LONG);
        expectedAttributeArray = new Attribute[]{symbolAttribute, totalVolumeAttribute, timeAttribute};
        siddhiAppRuntime.shutdown();
        AssertJUnit.assertArrayEquals(expectedAttributeArray, actualAttributeArray);

    }
}
