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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class which holds the utility methods which are used by various units in the Influxdb Event Table implementation.
 */

public class InfluxDBTableUtils {

    private InfluxDBTableUtils() {
        //preventing initialization
    }

    public static boolean isEmpty(String field) {

        return (field == null || field.trim().length() == 0);
    }

    /**
     * utility method to map tag values  to attributes before write to database;
     */
    public static Map<String, String> mapTagValuesToAttributes(Object[] record, List<String> attributeNames,
                                                               List<Integer> tagPositions) {

        Map<String, String> attributesValuesMap = new HashMap<>();
        for (int i = 0; i < record.length; i++) {
            for (int j = 0; j < tagPositions.size(); j++) {
                if (i == tagPositions.get(j)) {
                    attributesValuesMap.put(attributeNames.get(i), record[i].toString());
                }
            }
        }
        return attributesValuesMap;
    }

    /**
     * Method to map filed values to attributes before write to database.
     *
     * @param record         Object array of runtime values
     * @param attributeNames list of attribute names
     * @param tagPositions   list containing position of tag keys
     * @return
     */
    public static Map<String, Object> mapFieldValuesToAttributes(Object[] record, List<String> attributeNames,
                                                                 List<Integer> tagPositions) {

        Map<String, Object> attributesValuesMap = new HashMap<>();
        int count;
        for (int i = 0; i < record.length; i++) {
            count = 0;
            for (int j = 0; j < tagPositions.size(); j++) {
                if (i != tagPositions.get(j)) {
                    count++;
                }
            }
            if (count == tagPositions.size()) {
                attributesValuesMap.put(attributeNames.get(i), record[i]);
            }
        }
        return attributesValuesMap;
    }

    /**
     * Utility method to map time value to attributes.
     *
     * @param record        Object array of runtime values
     * @param tableName     name of the table
     * @param timePositions contains position of time attribute
     * @return
     */

    public static long mapTimeToAttributeValue(Object[] record, String tableName,
                                               int timePositions) {

        String time = "";
        try {
            for (int i = 0; i < record.length; i++) {
                if (i == timePositions) {
                    time = record[i].toString();
                }
            }
            long times = Long.parseLong(time);
            return times;
        } catch (NumberFormatException e) {
            throw new InfluxDBTableException("Specify 'time' attribute in type Long in the table definition "
                    + e.getMessage() + " in " + tableName, e);
        }
    }
}
