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

public class DeleteFromInfluxDBTestCase {

    private static final Log log = LogFactory.getLog(DeleteFromInfluxDBTestCase.class);

    @BeforeClass
    public static void startTest() {

        log.info("== InfluxDB Table DELETE tests started ==");
    }

    @AfterClass
    public static void shutdown() {

        log.info("== InfluxDB Table DELETE tests completed ==");
    }

    @BeforeMethod
    public void init() {

        try {
            InfluxDBTestUtils.initDatabaseTable(TABLE_NAME);
        } catch (InfluxDBException e) {
            log.info("Test case ignored due to " + e.getMessage());
        }
    }

    @Test(description = "Delete From InfluxDBTable successfully test1")
    public void deleteFromInfluxDBTableTestWithSingleCondition() throws InterruptedException, InfluxDBException {

        log.info("deleteFromTableTestWithSingleCondition");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long,time long ); " +
                "define stream DeleteStockStream (symbol string); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
                + "\")\n" +
                "@index(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long,time long ); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == symbol ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L, 1548181800000L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L, 1548181800000L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L, 1548181800006L});
        stockStream.send(new Object[]{"CSC", 157.6F, 100L, 1548181800006L});

        deleteStockStream.send(new Object[]{"WSO2"});
        deleteStockStream.send(new Object[]{"IBM"});
        Thread.sleep(1000);

        siddhiAppRuntime.shutdown();

        int pointsInTable = InfluxDBTestUtils.getPointsCount(TABLE_NAME);
        Assert.assertEquals(pointsInTable, 1, "deletion failed");
    }

    @Test(description = "Delete From InfluxDB Table successfully test2")
    public void deleteFromTableTestWithSingleCondition() throws InterruptedException, InfluxDBException {

        log.info("deleteFromTableTestWithSingleCondition");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long,time long ); " +
                "define stream DeleteStockStream (symbols string); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
                + "\")\n" +
                "@index(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long,time long ); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on  symbols == StockTable.symbol;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L, 1548181800000L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L, 1548181800000L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L, 1548181800006L});
        stockStream.send(new Object[]{"CSC", 158.6F, 100L, 1548181800006L});

        deleteStockStream.send(new Object[]{"WSO2"});
        deleteStockStream.send(new Object[]{"IBM"});
        Thread.sleep(1000);

        siddhiAppRuntime.shutdown();

        int pointsInTable = InfluxDBTestUtils.getPointsCount(TABLE_NAME);
        Assert.assertEquals(pointsInTable, 1, "deletion failed");
    }

    @Test(description = "Delete From influxDB table successfully test3")
    public void deleteFromInfluxDBTableTest() throws InterruptedException, InfluxDBException {

        log.info("deleteFromInfluxDBTableTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string,symbol2 String, price float, volume long,time long ); " +
                "define stream DeleteStockStream (symbol string,symbol2 string); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
                + "\")\n" +
                "@index(\"symbol\",\"symbol2\")" +
                "define table StockTable (symbol string,symbol2 string, price float, volume long,time long ); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == symbol and StockTable.symbol2==symbol2 ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", "WSO22", 55.6F, 100L, 1548181800000L});
        stockStream.send(new Object[]{"IBM", "IBM2", 75.6F, 100L, 1548181800000L});
        stockStream.send(new Object[]{"WSO2", "WSO22", 57.6F, 100L, 1548181800006L});
        stockStream.send(new Object[]{"CSC", "CSC2", 157.6F, 100L, 1548181800006L});

        deleteStockStream.send(new Object[]{"WSO2", "WSO22"});
        deleteStockStream.send(new Object[]{"IBM", "IBM2"});
        Thread.sleep(1000);

        siddhiAppRuntime.shutdown();

        int pointsInTable = InfluxDBTestUtils.getPointsCount(TABLE_NAME);
        Assert.assertEquals(pointsInTable, 1, "deletion failed");
    }

    @Test(description = "Delete From influxDB table successfully test4")
    public void deleteFromITableWithTwoConditionsTest() throws InterruptedException, InfluxDBException {

        log.info("deleteFromITableWithTwoConditionsTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string,symbol2 String, price float, volume long,time long ); " +
                "define stream DeleteStockStream (symbol string,symbol2 string); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
                + "\")\n" +

                "@index(\"symbol\",\"symbol2\")" +
                "define table StockTable (symbol string,symbol2 string, price float, volume long,time long ); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == symbol and StockTable.symbol2==symbol2 ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", "WSO22", 55.6F, 100L, 1548181800000L});
        stockStream.send(new Object[]{"IBM", "IBM2", 75.6F, 100L, 1548181800000L});
        stockStream.send(new Object[]{"WSO2", "WSO22", 57.6F, 100L, 1548181800006L});
        stockStream.send(new Object[]{"CSC", "CSC2", 157.6F, 100L, 1548181800006L});

        deleteStockStream.send(new Object[]{"WSO2", "WSO22"});
        deleteStockStream.send(new Object[]{"IBM", "IBM"});
        Thread.sleep(1000);

        siddhiAppRuntime.shutdown();

        int pointsInTable = InfluxDBTestUtils.getPointsCount(TABLE_NAME);
        Assert.assertEquals(pointsInTable, 2, "deletion failed");
    }
}
