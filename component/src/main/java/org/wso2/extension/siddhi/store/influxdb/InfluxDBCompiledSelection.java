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

import io.siddhi.core.util.collection.operator.CompiledSelection;

/**
 * Implementing class for a influxdb event table which Maintains the compiled select, group by, having  clauses.
 */
public class InfluxDBCompiledSelection implements CompiledSelection {

    private InfluxDBCompiledCondition compiledSelectClause;
    private InfluxDBCompiledCondition compiledGroupByClause;
    private InfluxDBCompiledCondition compiledHavingClause;
    private InfluxDBCompiledCondition compiledOrderByClause;
    private Long limit;
    private Long offset;

    public InfluxDBCompiledSelection(InfluxDBCompiledCondition compiledSelectClause,
                                     InfluxDBCompiledCondition compiledGroupByClause,
                                     InfluxDBCompiledCondition compiledHavingClause,
                                     InfluxDBCompiledCondition compiledOrderByClause, Long limit, Long offset) {
        this.compiledSelectClause = compiledSelectClause;
        this.compiledGroupByClause = compiledGroupByClause;
        this.compiledHavingClause = compiledHavingClause;
        this.compiledOrderByClause = compiledOrderByClause;
        this.limit = limit;
        this.offset = offset;
    }

    public InfluxDBCompiledCondition getCompiledSelectClause() {
        return compiledSelectClause;
    }

    public InfluxDBCompiledCondition getCompiledGroupByClause() {
        return compiledGroupByClause;
    }

    public InfluxDBCompiledCondition getCompiledHavingClause() {
        return compiledHavingClause;
    }

    public InfluxDBCompiledCondition getCompiledOrderByClause() {
        return compiledOrderByClause;
    }

    public Long getLimit() {
        return limit;
    }

    public Long getOffset() {
        return offset;
    }
}
