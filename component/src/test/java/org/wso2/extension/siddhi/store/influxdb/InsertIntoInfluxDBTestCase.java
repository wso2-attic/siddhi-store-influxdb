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
import org.wso2.siddhi.core.stream.input.InputHandler;

import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.DATABASE;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.PASSWORD;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.URL;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.USERNAME;

public class InsertIntoInfluxDBTestCase {

    private static final Log log = LogFactory.getLog(InsertIntoInfluxDBTestCase.class);

    @BeforeClass
    public static void startTest() {

        log.info("== InfluxDB Table INSERT tests started ==");
    }

    @AfterClass
    public static void shutdown() {

        log.info("==InfluxDB Table INSERT tests completed ==");
    }

    @BeforeMethod
    public void init() {

        try {
            InfluxDBTestUtils.initDatabaseTable(TABLE_NAME);
        } catch (InfluxDBException e) {
            log.info("Test case ignored due to " + e.getMessage());
        }
    }

    @Test(description = "Testing insertion with single tag key")
    public void insertIntoTableWithSingleTagKeyTest() throws InterruptedException, InfluxDBException {

        log.info("insertIntoTableWithSingleTagKeyTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long,time long); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
                + "\")\n" +

                "@Index(\"symbol\",\"time\")" +
                "define table StockTable (symbol string, price float, volume long,time long); ";

        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume,time\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 325.6f, 100L, 1548181800003L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L, 1548181800000L});
        Thread.sleep(500);

        int pointsInTable = InfluxDBTestUtils.getPointsCount(TABLE_NAME);
        Assert.assertEquals(pointsInTable, 2, "Definition/Insertion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing insertion with two tag keys")
    public void insertIntoTableWithTwoTagKeysTest() throws InterruptedException, InfluxDBException {

        log.info("insertIntoTableWithTwoTagKeysTest");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long,time long); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
                + "\")\n" +
                "@Index(\"symbol\",\"price\",\"time\")" +
                "define table StockTable (symbol string, price float, volume long,time long); ";

        String query2 = "" +
                "@info(name='query2') " +
                "from StockStream\n" +
                "select symbol,price,volume,time\n" +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query2);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 425.6f, 100L, 1548181800013L});
        siddhiAppRuntime.shutdown();

        int pointsInTable = InfluxDBTestUtils.getPointsCount(TABLE_NAME);
        Assert.assertEquals(pointsInTable, 1, "Insertion failed");
    }

    @Test(description = "Insert successfully with extracting current time")
    public void insertIntoTableTest() throws InterruptedException, InfluxDBException {

        log.info("InsertIntoInfluxDBTableTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
                + "\")\n" +
                "@Index(\"symbol\",\"time\")" +
                "define table StockTable (symbol string, price float, volume long,time long); ";

        String query2 = "" +
                "@info(name='query2') " +
                "from StockStream\n" +
                "select symbol,price,volume,currentTimeMillis() as time\n" +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query2);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 425.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 425.6f, 100L});
        siddhiAppRuntime.shutdown();

        int pointsInTable = InfluxDBTestUtils.getPointsCount(TABLE_NAME);
        Assert.assertEquals(pointsInTable, 2, "Insertion failed");
    }

    @Test(description = "define Table without tag keys")
    public void insertIntoTableWithoutTagKeysTest() throws InterruptedException, InfluxDBException {

        log.info("insertIntoTableWithoutTagKeysTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long,time long); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
                + "\")\n" +
                "@Index(\"time\")" +
                "define table StockTable (symbol string, price float, volume long,time long); ";

        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume,time\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 325.6f, 100L, 1548181800003L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L, 1548181800000L});

        int pointsInTable = InfluxDBTestUtils.getPointsCount(TABLE_NAME);
        Assert.assertEquals(pointsInTable, 2, "Definition/Insertion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "incorrect format for time")
    public void insertIntoTableWithIncorretTimeFormatTest() throws InterruptedException, InfluxDBException {

        log.info("insertIntoTableWithIncorretTimeFormatTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long,time string); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
                + "\")\n" +
                "@Index(\"symbol\",\"time\")" +
                "define table StockTable (symbol string, price float, volume long,time string); ";

        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume,time\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 325.6f, 100L, "vvv"});
        siddhiAppRuntime.shutdown();
    }
}
