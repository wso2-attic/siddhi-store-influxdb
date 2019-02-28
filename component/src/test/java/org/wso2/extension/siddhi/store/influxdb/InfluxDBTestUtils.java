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
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBException;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

public class InfluxDBTestUtils {

    public static final String DATABASE = "aTimeSeries";
    public static final String TABLE_NAME = "StockTable";
    public static final String URL = "http://localhost:8086";
    public static final String PASSWORD = "root";
    public static final String USERNAME = "root";
    private static final Log log = LogFactory.getLog(InfluxDBTestUtils.class);

    private InfluxDBTestUtils() {

    }

    public static void initDatabaseTable(String tableName) throws InfluxDBException {

        Query query;
        try {
            InfluxDB influxdb = InfluxDBFactory.connect(URL, USERNAME, PASSWORD);
            query = new Query("drop measurement " + tableName, DATABASE);
            influxdb.query(query);
        } catch (InfluxDBException e) {
            log.debug("clearing table failed due to" + e.getMessage());
        }
    }

    public static Integer getPointsCount(String tableName) {

        InfluxDB influxDB = InfluxDBFactory.connect(URL, USERNAME, PASSWORD);
        Query query1 = new Query("select count(volume) from " + TABLE_NAME, DATABASE);
        QueryResult queryResult1 = influxDB.query(query1);

        Object size = queryResult1
                .getResults()
                .get(0)
                .getSeries()
                .get(0)
                .getValues()
                .get(0)
                .get(1);

        int count = (int) ((double) size);
        return count;
    }
}
