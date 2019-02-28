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

/**
 * Class which holds the constants required by the Influxdb Event Table implementation.
 */

public class InfluxDBTableConstants {

    public static final String ANNOTATION_ELEMENT_URL = "url";
    public static final String ANNOTATION_ELEMENT_USERNAME = "username";
    public static final String ANNOTATION_ELEMENT_PASSWORD = "password";
    public static final String ANNOTATION_ELEMENT_TABLE_NAME = "table.name";
    public static final String ANNOTATION_ELEMENT_DATABASE = "database";

    public static final String INFLUXQL_MATH_ADD = "+";
    public static final String INFLUXQL_MATH_DIVIDE = "/";
    public static final String INFLUXQL_MATH_MULTIPLY = "*";
    public static final String INFLUXQL_MATH_SUBTRACT = "-";
    public static final String INFLUXQL_MATH_MOD = "%";
    public static final String INFLUXQL_COMPARE_LESS_THAN = "<";
    public static final String INFLUXQL_COMPARE_GREATER_THAN = ">";
    public static final String INFLUXQL_COMPARE_LESS_THAN_EQUAL = "<=";
    public static final String INFLUXQL_COMPARE_GREATER_THAN_EQUAL = ">=";
    public static final String INFLUXQL_COMPARE_EQUAL = "=";
    public static final String INFLUXQL_COMPARE_NOT_EQUAL = "<>";
    public static final String INFLUXQL_AND = "AND";
    public static final String INFLUXQL_OR = "OR";
    public static final String INFLUXQL_NOT = "NOT";
    public static final String INFLUXQL_IN = "IN";
    public static final String INFLUXQL_IS_NULL = "IS NULL";
    public static final String DELETE_QUERY = "DELETE FROM ";
    public static final String SELECT_QUERY = "SELECT * FROM ";
    public static final String INFLUXQL_WHERE = " WHERE ";

    public static final String WHITESPACE = " ";
    public static final String OPEN_PARENTHESIS = "(";
    public static final String CLOSE_PARENTHESIS = ")";

    private InfluxDBTableConstants() {
        //preventing initialization
    }

}
