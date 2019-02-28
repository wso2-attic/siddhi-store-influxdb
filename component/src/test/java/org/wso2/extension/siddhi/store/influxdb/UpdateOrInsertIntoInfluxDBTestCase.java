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

public class UpdateOrInsertIntoInfluxDBTestCase {

    private static final Log log = LogFactory.getLog(UpdateOrInsertIntoInfluxDBTestCase.class);

    @BeforeClass
    public static void startTest() {

        log.info("== InfluxDB Table UPDATE/INSERT tests started ==");
    }

    @AfterClass
    public static void shutdown() {

        log.info("== InfluxDB Table UPDATE/INSERT tests completed ==");
    }

    @BeforeMethod
    public void init() {

        try {
            InfluxDBTestUtils.initDatabaseTable(TABLE_NAME);
        } catch (InfluxDBException e) {
            log.info("Test case ignored due to " + e.getMessage());
        }
    }

    @Test(description = "Update or insert successfully with single condition")
    public void updateOrInsertWithSingleConditionTest() throws InterruptedException, InfluxDBException {

        log.info("updateOrInsertWithSingleConditionTest");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long,time long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long,time long); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
                + "\")\n" +
                "@Index(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long,time long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream#window.timeBatch(1 sec) " +
                "update or insert into StockTable " +
                "   on StockTable.symbol==symbol and StockTable.time==time;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 95.6F, 100L, 1548181800000L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L, 1548181800000L});
        stockStream.send(new Object[]{"WSO2", 67.6F, 100L, 1548181800500L});
        updateStockStream.send(new Object[]{"GOOG", 12.6F, 100L, 1548181800000L});
        updateStockStream.send(new Object[]{"IBM", 27.6F, 101L, 1548181800000L});
        Thread.sleep(3000);

        siddhiAppRuntime.shutdown();
        int pointsInTable = InfluxDBTestUtils.getPointsCount(TABLE_NAME);
        Assert.assertEquals(pointsInTable, 4, "updating failed");
    }

    @Test(description = "Upadate or insert successfully with single condition ")
    public void updateOrInsertWithSingleConditionTest2() throws InterruptedException, InfluxDBException {

        log.info("updateOrInsertWithSingleConditionTest2");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long,time long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long,time long); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
                + "\")\n" +
                "@Index(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long,time long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "update or insert into StockTable " +
                "   on StockTable.symbol=='IBM' and StockTable.time==time;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 95.6F, 100L, 1548181800000L});
        updateStockStream.send(new Object[]{"WSO2", 27.6F, 101L, 1548181800000L});
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
        int pointsInTable = InfluxDBTestUtils.getPointsCount(TABLE_NAME);
        Assert.assertEquals(pointsInTable, 1, "updating failed");
    }

    @Test(description = "Update or insert successfully with two conditions")
    public void updateOrInsertWithTwoConditionsTest() throws InterruptedException, InfluxDBException {

        log.info("updateOrInsertWithTwoConditionsTest");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long,time long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long,time long); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
                + "\")\n" +
                "@Index(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long,time long); ";
        String query = "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream#window.timeBatch(1 sec) " +
                "update or insert into StockTable " +
                "   on StockTable.symbol==symbol and StockTable.time==time;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();

        updateStockStream.send(new Object[]{"GOOG", 12.6F, 100L, 1548181800000L});
        updateStockStream.send(new Object[]{"IBM", 27.6F, 101L, 1548181800000L});
        updateStockStream.send(new Object[]{"WSO2", 27.6F, 101L, 1548181800500L});
        Thread.sleep(3000);

        siddhiAppRuntime.shutdown();
        int pointsInTable = InfluxDBTestUtils.getPointsCount(TABLE_NAME);
        Assert.assertEquals(pointsInTable, 3, "updating failed");
    }
}
