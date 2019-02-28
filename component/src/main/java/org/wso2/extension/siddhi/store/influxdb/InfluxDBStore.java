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

import org.influxdb.InfluxDBIOException;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.table.record.AbstractQueryableRecordTable;
import org.wso2.siddhi.core.table.record.ExpressionBuilder;
import org.wso2.siddhi.core.table.record.RecordIterator;
import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;
import org.wso2.siddhi.core.util.collection.operator.CompiledExpression;
import org.wso2.siddhi.core.util.collection.operator.CompiledSelection;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.annotation.Annotation;

import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.TableDefinition;
import org.wso2.siddhi.query.api.util.AnnotationHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTableConstants.ANNOTATION_ELEMENT_DATABASE;

import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTableConstants.ANNOTATION_ELEMENT_PASSWORD;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTableConstants.ANNOTATION_ELEMENT_TABLE_NAME;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTableConstants.ANNOTATION_ELEMENT_URL;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTableConstants.ANNOTATION_ELEMENT_USERNAME;

import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTableConstants.DELETE_QUERY;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTableConstants.INFLUXQL_WHERE;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTableConstants.SELECT_QUERY;
import static org.wso2.siddhi.core.util.SiddhiConstants.ANNOTATION_INDEX;
import static org.wso2.siddhi.core.util.SiddhiConstants.ANNOTATION_STORE;

/**
 * Class representing the InfluxDB store implementation
 */

@Extension(
        name = "influxdb",
        namespace = "store",
        description = "This extension assigns connection instructions to configure influxDB store. " +
                "It also implements read-write operations on connected influxDB database.",
        parameters = {
                @Parameter(
                        name = "url",
                        description = " Url via which the InfluxDB data store is accessed",
                        defaultValue = "http://localhost:8086 ",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "username",
                        description = " The username used to access InfluxDB data store ",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "password",
                        description = " The password used to access InfluxDB data store ",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "database",
                        description = " The database to which the data should be entered. ",
                        type = {DataType.STRING}
                ),
                @Parameter(name = "table.name",
                        description = "The name with which the event table should be persisted in the store. If no " +
                                "name is specified via this parameter, the event table is persisted with the same " +
                                "name as the Siddhi table.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "The table name defined in the Siddhi App query."),
        },
        examples = {
                @Example(
                        syntax = " @Store(type = \"influxdb\",\n" +
                                "   url = \"http://localhost:8086\",\n" +
                                "   username = \"root\",\n" +
                                "   password = \"root\" ,\n" +
                                "   database =\"aTimeSeries\")\n" +
                                "@Index(\"symbol\",\"time\")\n" +
                                "define table StockTable(symbol string,volume long,price float,time long) ;\n" +
                                "define stream StockStream (symbol string,volume long,price float);" +
                                "@info(name='query2') " +
                                "from StockStream\n" +
                                "select symbol,price,volume,currentTimeMillis() as time\n" +
                                "insert into StockTable ;",
                        description = " The above example creates a table in the  database if it does not " +
                                "exist with symbol which is configured under '@Index annotation as a tag key" +
                                ", 'volume ' and 'price' as field keys and time as the timestamp associated with" +
                                " particular data.Here the time is not needed to be given by the user and it extract " +
                                "the current time of the siddhi application."
                ),
                @Example(
                        syntax = " @Store(type = \"influxdb\",\n" +
                                "   url = \"http://localhost:8086\",\n" +
                                "   username = \"root\",\n" +
                                "   password = \"root\" ,\n" +
                                "   database =\"aTimeSeries\")\n" +
                                "@Index(\"symbol\",\"time\")\n" +
                                "define table StockTable(symbol string,volume long,price float,time long) ;\n" +
                                "define stream StockStream (symbol string,volume long,price float,time long);\n" +
                                "@info(name = 'query1')  \n" +
                                "from StockStream \n" +
                                "select symbol,  volume, price, time\n" +
                                "insert into StockTable ;",
                        description = " The above example creates a table in the  database if it does not exist" +
                                " already with 'symbol which is configured under '@Index' annotation  as a tag key " +
                                "and 'volume' and 'price' as field keys . Time represents the timestamp" +
                                "  associated with a particular data."

                ),
                @Example(
                        syntax = " @Store(type = \"influxdb\",\n" +
                                "   url = \"http://localhost:8086\",\n" +
                                "   username = \"root\",\n" +
                                "   password = \"root\" ,\n" +
                                "   database =\"aTimeSeries\")\n" +
                                "@Index(\"symbol\",\"time\")\n" +
                                "define table StockTable(symbol string,volume long,price float,time long) ;\n" +
                                "@info(name = 'query4')\n" +
                                "from ReadStream#window.length(1) join StockTable on " +
                                "StockTable.symbol==ReadStream.symbols \n" +
                                "select StockTable.symbol as checkName, StockTable.price as checkCategory,\n" +
                                "StockTable.volume as checkVolume,StockTable.time as checkTime\n" +
                                "insert into OutputStream; ",

                        description = " The above example creates a table in the database if it does not exist" +
                                " already with 'symbol' which is configured under '@Index' annotation  as a " +
                                "tag key , 'volume' and 'price' as field keys and time represents the timestamp" +
                                "Then the table is joined with a stream named 'ReadStream' based on a condition."
                )
        }
)

public class InfluxDBStore extends AbstractQueryableRecordTable {

    private static final Log log = LogFactory.getLog(InfluxDBStore.class);

    private String tableName;
    private String database;
    private String url;
    private String username;
    private String password;
    private int timePosition;
    private Annotation storeAnnotation;
    private Annotation indices;
    private InfluxDB influxdb;
    private List<String> attributeNames;
    private List<Integer> tagPositions;

    /**
     * Initializing the Record Table
     *
     * @param tableDefinition definintion of the table with annotations if any
     * @param configReader    this hold the {@link AbstractQueryableRecordTable} configuration reader.
     */

    @Override
    protected void init(TableDefinition tableDefinition, ConfigReader configReader) {

        attributeNames =
                tableDefinition.getAttributeList().stream().map(Attribute::getName).
                        collect(Collectors.toList());
        storeAnnotation = AnnotationHelper.getAnnotation(ANNOTATION_STORE, tableDefinition.getAnnotations());
        indices = AnnotationHelper.getAnnotation(ANNOTATION_INDEX, tableDefinition.getAnnotations());
        database = storeAnnotation.getElement(ANNOTATION_ELEMENT_DATABASE);
        url = storeAnnotation.getElement(ANNOTATION_ELEMENT_URL);
        username = storeAnnotation.getElement(ANNOTATION_ELEMENT_USERNAME);
        password = storeAnnotation.getElement(ANNOTATION_ELEMENT_PASSWORD);

        if (InfluxDBTableUtils.isEmpty(url)) {
            throw new SiddhiAppCreationException("Required parameter '" + ANNOTATION_ELEMENT_URL + "' for DB " +
                    "connectivity cannot be empty.");
        }
        if (InfluxDBTableUtils.isEmpty(username)) {
            throw new SiddhiAppCreationException("Required parameter '" + ANNOTATION_ELEMENT_USERNAME + "' for DB " +
                    "connectivity cannot be empty.");
        }

        if (InfluxDBTableUtils.isEmpty(password)) {
            throw new SiddhiAppCreationException("Required parameter '" + ANNOTATION_ELEMENT_PASSWORD + "' for DB " +
                    "connectivity cannot be empty.");
        }
        if (InfluxDBTableUtils.isEmpty(database)) {
            throw new SiddhiAppCreationException("Required parameter '" + ANNOTATION_ELEMENT_DATABASE + "'for DB "
                    + "connectivity cannot be empty.");
        }

        tagPositions = new ArrayList<>();

        if (indices != null) {
            indices.getElements().forEach(elem -> {
                for (int i = 0; i < this.attributeNames.size(); i++) {
                    if (this.attributeNames.get(i).equalsIgnoreCase("time")) {
                        timePosition = i;
                    } else if (this.attributeNames.get(i).equalsIgnoreCase(elem.getValue())) {
                        tagPositions.add(i);
                    }
                }
            });
        }

        String tableName = storeAnnotation.getElement(ANNOTATION_ELEMENT_TABLE_NAME);
        this.tableName = InfluxDBTableUtils.isEmpty(tableName) ? tableDefinition.getId() : tableName;
    }

    /**
     * Add records to the Table.
     *
     * @param records records that need to be added to the table, each Object[] represent a record and it will match
     *                the attributes of the Table Definition.
     */
    @Override
    protected void add(List<Object[]> records) {

        try {
            this.writePoints(records);
        } catch (InfluxDBException e) {
            throw new InfluxDBTableException("Error writing to table " + this.tableName + " : " + e.getMessage(), e);
        }
    }

    /**
     * Find records matching the compiled condition
     *
     * @param findConditionParameterMap map of matching StreamVariable Ids and their values corresponding to the
     *                                  compiled condition
     * @param compiledCondition         the compiledCondition against which records should be matched
     * @return RecordIterator of matching records
     */

    @Override
    protected RecordIterator<Object[]> find(Map<String, Object> findConditionParameterMap,
                                            CompiledCondition compiledCondition) {

        InfluxDBCompiledCondition influxCompiledCondition = (InfluxDBCompiledCondition) compiledCondition;
        String condition = influxCompiledCondition.getCompiledQuery();
        Map<Integer, Object> constantMap = influxCompiledCondition.getParametersConstants();

        try {
            QueryResult queryResult = this.getSelectQueryResult(findConditionParameterMap, condition, constantMap);
            return new InfluxDBIterator(queryResult);

        } catch (InfluxDBException e) {
            throw new InfluxDBTableException("Error retrieving records from table : '" + this.tableName + "': "
                    + e.getMessage(), e);
        }
    }

    /**
     * Check if matching record exist or not
     *
     * @param containsConditionParameterMap map of matching StreamVariable Ids and their values corresponding to the
     *                                      compiled condition
     * @param compiledCondition             the compiledCondition against which records should be matched
     * @return if matching record found or not
     */
    @Override
    protected boolean contains(Map<String, Object> containsConditionParameterMap,
                               CompiledCondition compiledCondition) {

        InfluxDBCompiledCondition influxCompiledCondition = (InfluxDBCompiledCondition) compiledCondition;
        String condition = influxCompiledCondition.getCompiledQuery();
        Map<Integer, Object> constantMap = influxCompiledCondition.getParametersConstants();

        try {
            QueryResult queryResult = this.getSelectQueryResult(containsConditionParameterMap, condition, constantMap);

            if (queryResult.getResults().get(0).getSeries() == null) {
                return false;
            } else {
                return true;
            }

        } catch (InfluxDBException e) {
            throw new InfluxDBTableException("Error performing 'contains'  on the table : '" + this.tableName + "': "
                    + e.getMessage(), e);
        }
    }

    /**
     * Delete all matching records.
     *
     * @param deleteConditionParameterMaps map of matching StreamVariable Ids and their values corresponding to the
     *                                     compiled condition
     * @param compiledCondition            the compiledCondition against which records should be matched for deletion
     **/
    @Override
    protected void delete(List<Map<String, Object>> deleteConditionParameterMaps, CompiledCondition compiledCondition) {

        InfluxDBCompiledCondition influxCompiledCondition = (InfluxDBCompiledCondition) compiledCondition;
        String condition = influxCompiledCondition.getCompiledQuery();
        Map<Integer, Object> constantMap = influxCompiledCondition.getParametersConstants();
        String findCondition;

        influxdb.setDatabase(database);
        Query query;

        try {
            StringBuilder lines = new StringBuilder();
            lines.append(DELETE_QUERY).append(this.tableName).append(INFLUXQL_WHERE);
            findCondition = lines.toString();

            for (Map<String, Object> map : deleteConditionParameterMaps) {
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    condition = condition.replaceFirst(Pattern.quote("?"), entry.getValue().toString());
                }
            }
            for (Map.Entry<Integer, Object> entry : constantMap.entrySet()) {
                Object myObject = entry.getValue();
                condition = condition.replaceFirst(Pattern.quote("*"), myObject.toString());
            }
            findCondition = findCondition + condition;

            query = new Query(findCondition, database);
            influxdb.query(query);

        } catch (InfluxDBException e) {
            throw new InfluxDBTableException("Error deleting records from table '" + this.tableName + "': "
                    + e.getMessage(), e);
        }
    }

    @Override
    protected void update(CompiledCondition compiledCondition, List<Map<String, Object>> list,
                          Map<String, CompiledExpression> map, List<Map<String, Object>> list1)
            throws ConnectionUnavailableException {

    }

    /**
     * Try updating the records if they exist else add the records
     *
     * @param list              map of matching StreamVariable Ids and their values corresponding to the
     *                          compiled condition based on which the records will be updated.
     * @param compiledCondition the compiledCondition against which records should be matched for update
     * @param map               the attributes and values that should be updated if the condition matches
     * @param listToAdd         the values for adding new records if the update condition did not match
     * @param listToUpdate      the attributes and values that should be updated for the matching records
     */
    @Override
    protected void updateOrAdd(CompiledCondition compiledCondition, List<Map<String, Object>> list,
                               Map<String, CompiledExpression> map, List<Map<String, Object>> listToAdd,
                               List<Object[]> listToUpdate) {

        try {

            this.writePoints(listToUpdate);

        } catch (InfluxDBException e) {
            throw new InfluxDBTableException("Error in updating or inserting records to table '" + this.tableName
                    + "': " + e.getMessage(), e);
        }
    }

    /**
     * Compile the matching condition
     *
     * @param expressionBuilder that helps visiting the conditions in order to compile the condition
     * @return compiled condition that can be used for matching events in find, contains, delete, update and
     * updateOrAdd
     */
    @Override
    protected CompiledCondition compileCondition(ExpressionBuilder expressionBuilder) {

        InfluxDBConditionVisitor visitor = new InfluxDBConditionVisitor();
        expressionBuilder.build(visitor);
        return new InfluxDBCompiledCondition(visitor.returnCondition(), visitor.getParameters(),
                visitor.getParametersConstant());

    }

    /**
     * Compile the matching condition
     *
     * @param expressionBuilder that helps visiting the conditions in order to compile the condition
     * @return compiled condition that can be used for matching events in find, contains, delete, update and
     * updateOrAdd
     */
    @Override
    protected CompiledExpression compileSetAttribute(ExpressionBuilder expressionBuilder) {

        return compileCondition(expressionBuilder);
    }

    /**
     * This method will be called before the processing method.
     * Intention to establish connection to publish event.
     *
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    protected void connect() throws ConnectionUnavailableException {

        try {
            influxdb = InfluxDBFactory.connect(url, username, password);
            if (!this.checkDatabaseExists(database)) {

                throw new ConnectionUnavailableException("database " + database + " does not exist");
            }
            influxdb.setDatabase(database);
        } catch (InfluxDBIOException e) {
            throw new ConnectionUnavailableException("failed to initialize influxDB store " + e.getMessage(), e);
        } catch (InfluxDBException.AuthorizationFailedException e) {
            throw new ConnectionUnavailableException("wrong url or username " + e.getMessage(), e);
        }

    }

    @Override
    protected void disconnect() {

    }

    @Override
    protected void destroy() {

    }

    public QueryResult getSelectQueryResult(Map<String, Object> selectConditionParameterMap,
                                            String condition, Map<Integer, Object> constantMap) {

        String findCondition;

        influxdb.setDatabase(database);
        Query query;
        QueryResult queryResult;

        StringBuilder line = new StringBuilder();
        line.append(SELECT_QUERY).append(this.tableName);

        if (condition.equals("'*'")) {
            findCondition = line.toString();

        } else {
            line.append(INFLUXQL_WHERE);

            for (Map.Entry<String, Object> entry : selectConditionParameterMap.entrySet()) {
                Object myObject = entry.getValue();
                if (myObject instanceof String) {
                    condition = condition.replaceFirst(Pattern.quote("?"), myObject.toString());
                } else {
                    condition = condition.replaceFirst(Pattern.quote("'?'"), myObject.toString());
                }
            }
            for (Map.Entry<Integer, Object> entry : constantMap.entrySet()) {
                Object myObject = entry.getValue();

                if (myObject instanceof String) {
                    condition = condition.replaceFirst(Pattern.quote("*"), myObject.toString());
                } else {
                    condition = condition.replaceFirst(Pattern.quote("'*'"), myObject.toString());
                }
            }

            findCondition = line.toString();
            findCondition = findCondition + condition;
        }

        influxdb.setDatabase(database);
        query = new Query(findCondition, database);
        influxdb.query(query);
        queryResult = influxdb.query(query);
        return queryResult;
    }

    public void writePoints(List<Object[]> list) {

        for (Object[] record : list) {
            Map<String, String> insertTagMap = InfluxDBTableUtils.mapTagValuesToAttributes(record,
                    this.attributeNames, this.tagPositions);
            Map<String, Object> insertFieldMap = InfluxDBTableUtils.mapFieldValuesToAttributes(record,
                    this.attributeNames, this.tagPositions);
            long times = InfluxDBTableUtils.mapTimeToAttributeValue(record, this.tableName,
                    this.timePosition);

            influxdb.setDatabase(database);
            BatchPoints batchPoints = BatchPoints.database(database).build();
            Point point = Point.measurement(tableName)
                    .time(times, TimeUnit.MILLISECONDS)
                    .tag(insertTagMap)
                    .fields(insertFieldMap)
                    .build();
            batchPoints.point(point);
            influxdb.write(batchPoints);
        }
    }

    public boolean checkDatabaseExists(String dbName) {

        boolean isExists = false;
        Query query = new Query("SHOW DATABASES", database);
        QueryResult queryResult = influxdb.query(query);
        List<List<Object>> databaseList = queryResult.getResults()
                .get(0)
                .getSeries()
                .get(0)
                .getValues();

        for (int i = 0; i < databaseList.size(); i++) {
            if (databaseList.get(i).get(0).equals(dbName)) {
                isExists = true;
            } else {
                isExists = false;
            }
        }

        return isExists;
    }

    @Override
    protected RecordIterator<Object[]> query(Map<String, Object> parameterMap, CompiledCondition compiledCondition,
                                             CompiledSelection compiledSelection, Attribute[] outputAttributes)
            throws ConnectionUnavailableException {

        return null;
    }

    @Override
    protected CompiledSelection compileSelection(List<SelectAttributeBuilder> selectAttributeBuilders,
                                                 List<ExpressionBuilder> groupByExpressionBuilder,
                                                 ExpressionBuilder havingExpressionBuilder,
                                                 List<OrderByAttributeBuilder> orderByAttributeBuilders, Long limit,
                                                 Long offset) {

        return null;
    }
}
