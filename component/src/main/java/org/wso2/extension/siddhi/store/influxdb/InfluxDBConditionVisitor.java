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

import org.wso2.siddhi.core.table.record.BaseExpressionVisitor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.expression.condition.Compare;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Class which is used by the Siddhi runtime for instructions on converting the SiddhiQL condition to the condition
 * format understood by the  InfluxDB
 */

public class InfluxDBConditionVisitor extends BaseExpressionVisitor {

    private StringBuilder condition;
    private String finalCompiledCondition;
    private int streamVarCount;
    private int constantCount;

    private Map<String, Object> placeholders;
    private Map<String, Object> placeholdersConstant;
    private SortedMap<Integer, Object> parameters;
    private SortedMap<Integer, Object> parametersCon;

    public InfluxDBConditionVisitor() {

        this.condition = new StringBuilder();
        this.streamVarCount = 0;
        this.constantCount = 0;
        this.placeholders = new HashMap<>();
        this.placeholdersConstant = new HashMap<>();
        this.parameters = new TreeMap<>();
        this.parametersCon = new TreeMap<>();
    }

    public String returnCondition() {

        this.parametrizeCondition();
        return this.finalCompiledCondition.trim();
    }

    public SortedMap<Integer, Object> getParameters() {

        return this.parameters;
    }

    public SortedMap<Integer, Object> getParametersConstant() {

        return this.parametersCon;
    }

    @Override
    public void beginVisitAnd() {

        condition.append(InfluxDBTableConstants.OPEN_PARENTHESIS);
    }

    @Override
    public void endVisitAnd() {

        condition.append(InfluxDBTableConstants.CLOSE_PARENTHESIS);
    }

    @Override
    public void beginVisitAndLeftOperand() {
        //Not applicable
    }

    @Override
    public void endVisitAndLeftOperand() {
        //Not applicable
    }

    @Override
    public void beginVisitAndRightOperand() {

        condition.append(InfluxDBTableConstants.WHITESPACE)
                .append(InfluxDBTableConstants.INFLUXQL_AND)
                .append(InfluxDBTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitAndRightOperand() {
        //Not applicable
    }

    @Override
    public void beginVisitOr() {

        condition.append(InfluxDBTableConstants.OPEN_PARENTHESIS);
    }

    @Override
    public void endVisitOr() {

        condition.append(InfluxDBTableConstants.CLOSE_PARENTHESIS);
    }

    @Override
    public void beginVisitOrLeftOperand() {
        //Not applicable
    }

    @Override
    public void endVisitOrLeftOperand() {
        //Not applicable
    }

    @Override
    public void beginVisitOrRightOperand() {

        condition.append(InfluxDBTableConstants.INFLUXQL_OR).append(InfluxDBTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitOrRightOperand() {
        //Not applicable
    }

    @Override
    public void beginVisitNot() {

        condition.append(InfluxDBTableConstants.INFLUXQL_NOT).append(InfluxDBTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitNot() {
        //Not applicable
    }

    @Override
    public void beginVisitCompare(Compare.Operator operator) {

        condition.append(InfluxDBTableConstants.OPEN_PARENTHESIS);
    }

    @Override
    public void endVisitCompare(Compare.Operator operator) {

        condition.append(InfluxDBTableConstants.CLOSE_PARENTHESIS);
    }

    @Override
    public void beginVisitCompareLeftOperand(Compare.Operator operator) {
        //Not applicable
    }

    @Override
    public void endVisitCompareLeftOperand(Compare.Operator operator) {
        //Not applicable
    }

    @Override
    public void beginVisitCompareRightOperand(Compare.Operator operator) {

        switch (operator) {
            case EQUAL:
                condition.append(InfluxDBTableConstants.INFLUXQL_COMPARE_EQUAL);
                break;
            case GREATER_THAN:
                condition.append(InfluxDBTableConstants.INFLUXQL_COMPARE_GREATER_THAN);
                break;
            case GREATER_THAN_EQUAL:
                condition.append(InfluxDBTableConstants.INFLUXQL_COMPARE_GREATER_THAN_EQUAL);
                break;
            case LESS_THAN:
                condition.append(InfluxDBTableConstants.INFLUXQL_COMPARE_LESS_THAN);
                break;
            case LESS_THAN_EQUAL:
                condition.append(InfluxDBTableConstants.INFLUXQL_COMPARE_LESS_THAN_EQUAL);
                break;
            case NOT_EQUAL:
                condition.append(InfluxDBTableConstants.INFLUXQL_COMPARE_NOT_EQUAL);
                break;
        }
        condition.append(InfluxDBTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitCompareRightOperand(Compare.Operator operator) {
        //Not applicable
    }

    @Override
    public void beginVisitIsNull(String streamId) {

    }

    @Override
    public void endVisitIsNull(String streamId) {

        condition.append(InfluxDBTableConstants.INFLUXQL_IS_NULL).append(InfluxDBTableConstants.WHITESPACE);
    }

    @Override
    public void beginVisitIn(String storeId) {

        condition.append(InfluxDBTableConstants.INFLUXQL_IN).append(InfluxDBTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitIn(String storeId) {
        //Not applicable
    }

    @Override
    public void beginVisitConstant(Object value, Attribute.Type type) {

        String name;
        name = this.generateConstantName();
        this.placeholdersConstant.put(name, value);
        condition.append("[").append(name).append("]").append(InfluxDBTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitConstant(Object value, Attribute.Type type) {
        //Not applicable
    }

    @Override
    public void beginVisitMath(MathOperator mathOperator) {

        condition.append(InfluxDBTableConstants.OPEN_PARENTHESIS);
    }

    @Override
    public void endVisitMath(MathOperator mathOperator) {

        condition.append(InfluxDBTableConstants.CLOSE_PARENTHESIS);
    }

    @Override
    public void beginVisitMathLeftOperand(MathOperator mathOperator) {
        //Not applicable
    }

    @Override
    public void endVisitMathLeftOperand(MathOperator mathOperator) {
        //Not applicable
    }

    @Override
    public void beginVisitMathRightOperand(MathOperator mathOperator) {

        switch (mathOperator) {
            case ADD:
                condition.append(InfluxDBTableConstants.INFLUXQL_MATH_ADD);
                break;
            case DIVIDE:
                condition.append(InfluxDBTableConstants.INFLUXQL_MATH_DIVIDE);
                break;
            case MOD:
                condition.append(InfluxDBTableConstants.INFLUXQL_MATH_MOD);
                break;
            case MULTIPLY:
                condition.append(InfluxDBTableConstants.INFLUXQL_MATH_MULTIPLY);
                break;
            case SUBTRACT:
                condition.append(InfluxDBTableConstants.INFLUXQL_MATH_SUBTRACT);
                break;
        }
        condition.append(InfluxDBTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitMathRightOperand(MathOperator mathOperator) {
        //Not applicable
    }

    @Override
    public void beginVisitAttributeFunction(String namespace, String functionName) {

    }

    @Override
    public void endVisitAttributeFunction(String namespace, String functionName) {

    }

    @Override
    public void beginVisitParameterAttributeFunction(int index) {
        //Not applicable
    }

    @Override
    public void endVisitParameterAttributeFunction(int index) {
        //Not applicable
    }

    @Override
    public void beginVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {

        String name;
        name = this.generateStreamVarName();
        this.placeholders.put(name, new Attribute(id, type));
        condition.append("[").append(name).append("]").append(InfluxDBTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
        //Not applicable
    }

    @Override
    public void beginVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {

        condition.append("\"")
                .append(attributeName)
                .append("\"")
                .append(InfluxDBTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
        //Not applicable
    }

    private void parametrizeCondition() {

        String query = this.condition.toString();
        String[] tokens = query.split("\\[");
        int ordinal = 1;
        int ordinalCon = 1;
        for (String token : tokens) {
            if (token.contains("]")) {
                String candidate = token.substring(0, token.indexOf("]"));
                if (this.placeholders.containsKey(candidate)) {
                    this.parameters.put(ordinal, this.placeholders.get(candidate));
                    ordinal++;
                } else if (this.placeholdersConstant.containsKey(candidate)) {
                    this.parametersCon.put(ordinalCon, this.placeholdersConstant.get(candidate));
                    ordinalCon++;
                }
            }
        }
        for (String placeholder : this.placeholders.keySet()) {
            query = query.replace("[" + placeholder + "]", "'?'");
        }
        for (String placeholder : this.placeholdersConstant.keySet()) {
            query = query.replace("[" + placeholder + "]", "'*'");
        }

        this.finalCompiledCondition = query;
    }

    private String generateStreamVarName() {

        String name = "strVar" + this.streamVarCount;
        this.streamVarCount++;
        return name;
    }

    private String generateConstantName() {

        String name = "const" + this.constantCount;
        this.constantCount++;
        return name;
    }

}
