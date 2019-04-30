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

import io.siddhi.core.table.record.RecordIterator;
import org.influxdb.InfluxDBException;
import org.influxdb.dto.QueryResult;

import java.io.IOException;
import java.util.List;

/**
 * A class representing a RecordIterator which is responsible for processing Influxdb Event Table find() operations in
 * a streaming fashion.
 */
public class InfluxDBIterator implements RecordIterator<Object[]> {

    private QueryResult queryResult;
    private int index = 0;
    private int indexSeries = 0;
    private List<List<Object>> result;
    private List<QueryResult.Series> resultSeries;

    public InfluxDBIterator(QueryResult queryResult) {
        this.queryResult = queryResult;
        if (queryResult.getResults().get(0).getSeries() != null) {
            this.resultSeries = queryResult.getResults().get(0).getSeries();
            this.result = queryResult.getResults().get(0).getSeries().get(indexSeries).getValues();
        } else {
            this.resultSeries = null;
        }
    }

    @Override
    public boolean hasNext() {
        this.result = queryResult.getResults().get(0).getSeries().get(indexSeries).getValues();
        if (result == null) {
            return false;
        } else if (result.size() > index) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Object[] next() {
        if (this.hasNext()) {
            return extractRecord(this.queryResult);
        } else {
            return new Object[0];
        }
    }

    /*
     * Method which is used for extracting record values (in the form of an Object array),
     * according to the table's field type order.
     */
    private Object[] extractRecord(QueryResult queryResult) throws InfluxDBException {
        List<Object> record = queryResult
                .getResults()
                .get(0)
                .getSeries()
                .get(indexSeries)
                .getValues()
                .get(index);
        index = index + 1;
        while (indexSeries < resultSeries.size() - 1) {
            if (index <= result.size()) {
                index = 0;
                indexSeries++;
            }
        }
        return record.toArray();
    }

    @Override
    public void close() throws IOException { }
}
