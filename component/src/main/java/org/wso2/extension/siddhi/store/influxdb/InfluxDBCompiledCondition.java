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

import io.siddhi.core.util.collection.operator.CompiledCondition;

import java.util.SortedMap;

/**
 * Implementation of class corresponding to the InfluxDB store
 */
public class InfluxDBCompiledCondition implements CompiledCondition {

    private String compiledQuery;
    private SortedMap<Integer, Object> parametersConstant;

    public InfluxDBCompiledCondition(String compiledQuery, SortedMap<Integer, Object> parameters,
                                     SortedMap<Integer, Object> parametersConstant) {
        this.compiledQuery = compiledQuery;
        this.parametersConstant = parametersConstant;
    }

    public String getCompiledQuery() {
        return compiledQuery;
    }

    public SortedMap<Integer, Object> getParametersConstants() {
        return parametersConstant;
    }
}
