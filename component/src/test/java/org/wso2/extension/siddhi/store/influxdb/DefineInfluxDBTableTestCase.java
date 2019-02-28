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
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.InputHandler;

import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.DATABASE;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.PASSWORD;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.URL;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTestUtils.USERNAME;

public class DefineInfluxDBTableTestCase {

    private static final Log log = LogFactory.getLog(DefineInfluxDBTableTestCase.class);

    @BeforeClass
    public static void startTest() {

        log.info("== InfluxDB table definition Test started ==");
    }

    @AfterClass
    public static void shutdown() {

        log.info("== InfluxDB table definition Test started ==");
    }

    @BeforeMethod
    public void init() {

        try {
            InfluxDBTestUtils.initDatabaseTable(TABLE_NAME);
        } catch (InfluxDBException e) {
            log.info("Test case ignored due to " + e.getMessage());
        }
    }

    @Test(testName = "influxDBTableCreationTest", description = "Testing table creation. ")
    public void influxDBTableCreationTest() throws InterruptedException, InfluxDBException {

        log.info("InfluxDBTableCreationTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long,time long); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
                + "\")\n" +
                "@Index(\"symbol\", \"time\")" +
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

    @Test(expectedExceptions = SiddhiAppCreationException.class, description = "testing without defining url value")
    public void influxDBTableDefinitionWithoutUrlTest() {

        log.info("InfluxDBTableDefinitionWithoutUrlTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long,time long); " +
                "@Store(type=\"influxdb\", url = \"" + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
                + "\")\n" +
                "@Index(\"symbol\", \"time\")" +
                "define table StockTable (symbol string, price float, volume long,time long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume,time\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class, description = "testing without defining username")
    public void influxDBTableWithoutUsernameTest() {

        log.info("InfluxDBTableDefinitionWithoutUsernameTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long,time long); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
                + "\")\n" +
                "@Index(\"symbol\", \"time\")" +
                "define table StockTable (symbol string, price float, volume long,time long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume,time\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class, description = "testing without defining password")
    public void influxDBTableCreationWithoutPasswordTest() {

        log.info("influxDBTableCreationWithoutPasswordTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long,time long); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + "\", database = \"" + DATABASE
                + "\")\n" +
                "@Index(\"symbol\", \"time\")" +
                "define table StockTable (symbol string, price float, volume long,time long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume,time\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class, description = "Testing without defining database name")
    public void influxDBTableCreationWithoutDatabaseTest() {

        log.info("InfluxDBTableCreationWithoutDatabaseTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long,time long); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \""
                + "\")\n" +
                "@Index(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long,time long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume,time\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

    }

    @Test(description = "testing with incorrect value for url")
    public void influxDBTablWithIncorrectUrlTest() {

        log.info("InfluxDBTablWithIncorrectUrlTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long,time long); " +
                "@Store(type=\"influxdb\", url = \"" + "http://localhost:8096" + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
                + "\")\n" +
                "@Index(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long,time long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume,time\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

    }

    @Test(description = "Testing with incorrect username")
    public void influxDBTableWithIncorrectUsernameTest() throws InterruptedException, InfluxDBException {

        log.info("InfluxDBTableWithIncorrectUsernameTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long,time long); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + "roott" + "\", password=\"" + PASSWORD + "\", database = \"" + DATABASE
                + "\")\n" +
                "@Index(\"symbol\", \"time\")" +
                "define table StockTable (symbol string, price float, volume long,time long); ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        Thread.sleep(500);
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing with incorrect password")
    public void influxDBTableDefinitionWithIncorrectPasswordTest() throws InfluxDBException {

        log.info("influxDBTableDefinitionWithIncorrectPasswordTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long,time long); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + "roott" + "\", database = \"" + DATABASE
                + "\")\n" +
                "@Index(\"symbol\", \"time\")" +
                "define table StockTable (symbol string, price float, volume long,time long); ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing with non existing database ")
    public void tableDefinitionWithNonExistingDatabaseTest() throws InfluxDBException {

        log.info("tableDefinitionWithNonExistingDatabaseTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long,time long); " +
                "@Store(type=\"influxdb\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + "root" + "\", database = \"" + "DATABASE"
                + "\")\n" +
                "@Index(\"symbol\", \"time\")" +
                "define table StockTable (symbol string, price float, volume long,time long); ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }
}
