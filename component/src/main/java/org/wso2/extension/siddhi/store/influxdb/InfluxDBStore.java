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
import org.wso2.siddhi.query.api.execution.query.selection.OrderByAttribute;
import org.wso2.siddhi.query.api.util.AnnotationHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTableConstants.ANNOTATION_ELEMENT_DATABASE;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTableConstants.ANNOTATION_ELEMENT_PASSWORD;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTableConstants.ANNOTATION_ELEMENT_RETENTION_POLICY;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTableConstants.ANNOTATION_ELEMENT_TABLE_NAME;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTableConstants.ANNOTATION_ELEMENT_URL;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTableConstants.ANNOTATION_ELEMENT_USERNAME;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTableConstants.DELETE_QUERY;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTableConstants.INFLUXQL_AS;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTableConstants.INFLUXQL_WHERE;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTableConstants.SELECT_QUERY;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTableConstants.SEPARATOR;
import static org.wso2.extension.siddhi.store.influxdb.InfluxDBTableConstants.WHITESPACE;
import static org.wso2.siddhi.core.util.SiddhiConstants.ANNOTATION_INDEX;
import static org.wso2.siddhi.core.util.SiddhiConstants.ANNOTATION_STORE;

/**
 * Class representing the InfluxDB store implementation
 */
@Extension(
        name = "influxdb",
        namespace = "store",
        description = "This extension connects to  influxDB store. " +
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
                        name = "influxdb.database",
                        description = " The name of the InfluxDB database to which the data should be entered. ",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "retention.policy",
                        description = " Describes how long InfluxDB keeps data. ",
                        optional = true,
                        defaultValue = "autogen",
                        type = {DataType.STRING}
                ),
                @Parameter(name = "table.name",
                        description = "The name with which the siddhi store  should be persisted in the InfluxDB " +
                                "database. If no name is specified via this parameter, the store is persisted in " +
                                "InfluxDB database with the same name define in the table definition of siddhi app.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "The table name defined in the Siddhi App query."),
        },
        examples = {
                @Example(
                        syntax = " @Store (type = \"influxdb\",\n" +
                                "   url = \"http://localhost:8086\",\n" +
                                "   username = \"root\",\n" +
                                "   password = \"root\" ,\n" +
                                "   database =\"aTimeSeries\")\n" +
                                "@Index (\"symbol\",\"time\")\n" +
                                "define table StockTable (symbol string,volume long,price float,time long) ;\n" +
                                "define stream StockStream (symbol string,volume long,price float);" +
                                "@info (name='query2') " +
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
                        syntax = " @Store (type = \"influxdb\",\n" +
                                "   url = \"http://localhost:8086\",\n" +
                                "   username = \"root\",\n" +
                                "   password = \"root\" ,\n" +
                                "   database =\"aTimeSeries\")\n" +
                                "@Index (\"symbol\",\"time\")\n" +
                                "define table StockTable (symbol string,volume long,price float,time long) ;\n" +
                                "define stream StockStream (symbol string,volume long,price float,time long);\n" +
                                "@info (name = 'query1')  \n" +
                                "from StockStream \n" +
                                "select symbol,  volume, price, time\n" +
                                "insert into StockTable ;",
                        description = " The above example creates a table in the  database if it does not exist" +
                                " already with 'symbol which is configured under '@Index' annotation  as a tag key " +
                                "and 'volume' and 'price' as field keys . Time represents the timestamp" +
                                "  associated with a particular data."

                ),
                @Example(
                        syntax = " @Store (type = \"influxdb\",\n" +
                                "   url = \"http://localhost:8086\",\n" +
                                "   username = \"root\",\n" +
                                "   password = \"root\" ,\n" +
                                "   database =\"aTimeSeries\")\n" +
                                "@Index (\"symbol\",\"time\")\n" +
                                "define table StockTable(symbol string,volume long,price float,time long) ;\n" +
                                "@info (name = 'query4')\n" +
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
    private String retention;
    private boolean connected;
    private int timePosition;
    private Annotation storeAnnotation;
    private Annotation indices;
    private InfluxDB influxdb;
    private List<String> attributeNames;
    private List<Integer> tagPositions;

    /**
     * Initializing the Record Table
     * @param tableDefinition definintion of the table with annotations if any
     * @param configReader    this hold the {@link AbstractQueryableRecordTable} configuration reader.
     */
    @Override
    protected void init(TableDefinition tableDefinition, ConfigReader configReader) {
        this.attributeNames =
                tableDefinition.getAttributeList().stream().map(Attribute::getName).
                        collect(Collectors.toList());
        storeAnnotation = AnnotationHelper.getAnnotation(ANNOTATION_STORE, tableDefinition.getAnnotations());
        indices = AnnotationHelper.getAnnotation(ANNOTATION_INDEX, tableDefinition.getAnnotations());
        database = storeAnnotation.getElement(ANNOTATION_ELEMENT_DATABASE);
        url = storeAnnotation.getElement(ANNOTATION_ELEMENT_URL);
        username = storeAnnotation.getElement(ANNOTATION_ELEMENT_USERNAME);
        password = storeAnnotation.getElement(ANNOTATION_ELEMENT_PASSWORD);
        String tableName = storeAnnotation.getElement(ANNOTATION_ELEMENT_TABLE_NAME);
        retention = storeAnnotation.getElement(ANNOTATION_ELEMENT_RETENTION_POLICY);
        this.tableName = InfluxDBTableUtils.isEmpty(tableName) ? tableDefinition.getId() : tableName;
        if (InfluxDBTableUtils.isEmpty(url)) {
            throw new SiddhiAppCreationException("Required parameter '" + ANNOTATION_ELEMENT_URL + "' for DB " +
                    "connectivity cannot be empty" + " for creating table : " + this.tableName);
        }
        if (InfluxDBTableUtils.isEmpty(username)) {
            throw new SiddhiAppCreationException("Required parameter '" + ANNOTATION_ELEMENT_USERNAME + "' for DB " +
                    "connectivity cannot be empty" + " for creating table : " + this.tableName);
        }
        if (InfluxDBTableUtils.isEmpty(password)) {
            throw new SiddhiAppCreationException("Required parameter '" + ANNOTATION_ELEMENT_PASSWORD + "' for DB " +
                    "connectivity cannot be empty " + " for creating table : " + this.tableName);
        }
        if (InfluxDBTableUtils.isEmpty(database)) {
            throw new SiddhiAppCreationException("Required parameter '" + ANNOTATION_ELEMENT_DATABASE + "'for DB "
                    + "connectivity cannot be empty " + " for creating table : " + this.tableName);
        }
        if (!InfluxDBTableUtils.validateTimeAttribute(attributeNames)) {
            throw new SiddhiAppCreationException("time attribute cannot be empty in table definition of table : "
                    + tableName);
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
        StringBuilder findCondition = new StringBuilder();
        Query query;
        try {
            findCondition.append(DELETE_QUERY).append(this.tableName);
            if (!condition.equals("'*'")) {
                findCondition.append(INFLUXQL_WHERE);
                for (Map<String, Object> map : deleteConditionParameterMaps) {
                    for (Map.Entry<String, Object> entry : map.entrySet()) {
                        condition = condition.replaceFirst(Pattern.quote("?"), entry.getValue().toString());
                    }
                }
                for (Map.Entry<Integer, Object> entry : constantMap.entrySet()) {
                    Object constant = entry.getValue();
                    condition = condition.replaceFirst(Pattern.quote("*"), constant.toString());
                }
            }
            findCondition.append(condition);
            query = new Query(findCondition.toString(), database);
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
        log.error("update operation is not defined for influxDB store implementation for " + this.tableName);
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
                connected = false;
                throw new InfluxDBTableException("database " + database + " does not exist");
            }
            if (!InfluxDBTableUtils.isEmpty(retention)) {
                influxdb.setRetentionPolicy(retention);
            }
            influxdb.setDatabase(database);
            connected = true;
        } catch (InfluxDBIOException e) {
            connected = false;
            throw new ConnectionUnavailableException("failed to initialize influxDB store " + e.getMessage(), e);
        } catch (InfluxDBException.AuthorizationFailedException e) {
            connected = false;
            throw new InfluxDBTableException("Either provided  username : " + username +
                    " or password "  + " is incorrect " + e.getMessage(), e);
        }
    }

    @Override
    protected void disconnect() {

        if (connected) {
            this.influxdb.close();
        }
    }

    @Override
    protected void destroy() {

        this.disconnect();
    }

    public QueryResult getSelectQueryResult(Map<String, Object> selectConditionParameterMap,
                                            String condition, Map<Integer, Object> constantMap) {
        Query query;
        StringBuilder findCondition = new StringBuilder();
        findCondition.append(SELECT_QUERY).append(this.tableName);
        if (!condition.equals("'*'")) {
            findCondition.append(INFLUXQL_WHERE);
            for (Map.Entry<String, Object> entry : selectConditionParameterMap.entrySet()) {
                Object streamVariable = entry.getValue();
                if (streamVariable instanceof String) {
                    condition = condition.replaceFirst(Pattern.quote("?"), streamVariable.toString());
                } else {
                    condition = condition.replaceFirst(Pattern.quote("'?'"), streamVariable.toString());
                }
            }
            for (Map.Entry<Integer, Object> entry : constantMap.entrySet()) {
                Object constant = entry.getValue();
                if (constant instanceof String) {
                    condition = condition.replaceFirst(Pattern.quote("*"), constant.toString());
                } else {
                    condition = condition.replaceFirst(Pattern.quote("'*'"), constant.toString());
                }
            }
            findCondition.append(condition);
        }
        query = new Query(findCondition.toString(), database);
        return influxdb.query(query);
    }

    public void writePoints(List<Object[]> list) {
        for (Object[] record : list) {
            Map<String, String> insertTagMap = InfluxDBTableUtils.mapTagValuesToAttributes(record,
                    this.attributeNames, this.tagPositions);
            Map<String, Object> insertFieldMap = InfluxDBTableUtils.mapFieldValuesToAttributes(record,
                    this.attributeNames, this.tagPositions);
            long times = InfluxDBTableUtils.mapTimeToAttributeValue(record, this.tableName,
                    this.timePosition);
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
        int index = 0;
        Query query = new Query("SHOW DATABASES", database);
        QueryResult queryResult = this.influxdb.query(query);
        List<List<Object>> databaseList = queryResult.getResults()
                .get(0)
                .getSeries()
                .get(0)
                .getValues();
        while (!isExists && index < databaseList.size()) {
            if (databaseList.get(index).get(0).equals(dbName)) {
                isExists = true;
            }
            index++;
        }
        return isExists;
    }

    @Override
    protected RecordIterator<Object[]> query(Map<String, Object> parameterMap, CompiledCondition compiledCondition,
                                             CompiledSelection compiledSelection, Attribute[] outputAttributes)
            throws ConnectionUnavailableException {
        InfluxDBCompiledSelection influxDBCompiledSelection = (InfluxDBCompiledSelection) compiledSelection;
        InfluxDBCompiledCondition influxDBCompiledCondition = (InfluxDBCompiledCondition) compiledCondition;
        Map<Integer, Object> consMap = influxDBCompiledCondition.getParametersConstants();
        Query queryInflux;
        String query = getSelectQuery(influxDBCompiledCondition, influxDBCompiledSelection, parameterMap, consMap);
        try {
            queryInflux = new Query(query, database);
        } catch (InfluxDBException e) {
            throw new InfluxDBTableException("Error when preparing to execute query: '" + query
                    + "' in store '" + this.tableName + "' : " + e.getMessage(), e);
        }
        try {
            return new InfluxDBIterator(influxdb.query(queryInflux));
        } catch (InfluxDBException e) {
            throw new InfluxDBTableException("Error when executing query: '" + query
                    + "' in store '" + this.tableName + "' : " + e.getMessage(), e);
        }
    }

    private String getSelectQuery(InfluxDBCompiledCondition influxDBCompiledCondition, InfluxDBCompiledSelection
            influxDBCompiledSelection, Map<String, Object> parameterMap, Map<Integer, Object> constantMap) {
        String selectors = influxDBCompiledSelection.getCompiledSelectClause().getCompiledQuery();
        String condition = influxDBCompiledCondition.getCompiledQuery();
        StringBuilder selectQuery = new StringBuilder("select ");
        selectQuery.append(selectors)
                .append(" from ").append(this.tableName);
        if (!condition.equals("'*'")) {
            selectQuery.append(INFLUXQL_WHERE);
            for (Map.Entry<String, Object> entry : parameterMap.entrySet()) {
                Object streamVariable = entry.getValue();
                if (streamVariable instanceof String) {
                    condition = condition.replaceFirst(Pattern.quote("?"), streamVariable.toString());
                } else {
                    condition = condition.replaceFirst(Pattern.quote("'?'"), streamVariable.toString());
                }
            }
            for (Map.Entry<Integer, Object> entry : constantMap.entrySet()) {
                Object constant = entry.getValue();
                if (constant instanceof String) {
                    condition = condition.replaceFirst(Pattern.quote("*"), constant.toString());
                } else {
                    condition = condition.replaceFirst(Pattern.quote("'*'"), constant.toString());
                }
            }
            selectQuery.append(condition);
        }
        InfluxDBCompiledCondition compiledGroupByClause = influxDBCompiledSelection.getCompiledGroupByClause();
        if (compiledGroupByClause != null) {
            String groupByClause = "group by " + compiledGroupByClause.getCompiledQuery();
            selectQuery.append(WHITESPACE).append(groupByClause);
        }
        if (influxDBCompiledSelection.getLimit() != null) {
            selectQuery.append(" limit ").append(influxDBCompiledSelection.getLimit());
        }
        if (influxDBCompiledSelection.getOffset() != null) {
            selectQuery.append(" offset ").append(influxDBCompiledSelection.getOffset());
        }
        InfluxDBCompiledCondition compiledHavingClause = influxDBCompiledSelection.getCompiledHavingClause();
        if (compiledHavingClause != null) {
            throw new InfluxDBTableException("Having clause is defined in the query for " + this.tableName +
                   " But it is not defined for influxDB store implementation  ");
        }
        return selectQuery.toString();
    }

    @Override
    protected CompiledSelection compileSelection(List<SelectAttributeBuilder> selectAttributeBuilders,
                                                 List<ExpressionBuilder> groupByExpressionBuilder,
                                                 ExpressionBuilder havingExpressionBuilder,
                                                 List<OrderByAttributeBuilder> orderByAttributeBuilders, Long limit,
                                                 Long offset) {
        return new InfluxDBCompiledSelection(
                compileSelectClause(selectAttributeBuilders),
                (groupByExpressionBuilder == null) ? null : compileClause(groupByExpressionBuilder),
                (havingExpressionBuilder == null) ? null :
                        compileClause(Collections.singletonList(havingExpressionBuilder)),
                (orderByAttributeBuilders == null) ? null : compileOrderByClause(orderByAttributeBuilders),
                limit, offset);
    }

    private InfluxDBCompiledCondition compileSelectClause(List<SelectAttributeBuilder> selectAttributeBuilders) {
        StringBuilder compiledSelectionList = new StringBuilder();
        SortedMap<Integer, Object> paramMap = new TreeMap<>();
        SortedMap<Integer, Object> paramCons = new TreeMap<>();
        int offset = 0;
        for (SelectAttributeBuilder selectAttributeBuilder : selectAttributeBuilders) {
            InfluxDBConditionVisitor visitor = new InfluxDBConditionVisitor();
            selectAttributeBuilder.getExpressionBuilder().build(visitor);
            String compiledCondition = visitor.returnCondition();
            compiledSelectionList.append(compiledCondition);
            if (selectAttributeBuilder.getRename() != null && !selectAttributeBuilder.getRename().isEmpty()) {
                compiledSelectionList.append(INFLUXQL_AS).
                        append(selectAttributeBuilder.getRename());
            }
            compiledSelectionList.append(SEPARATOR);
            Map<Integer, Object> conditionParamMap = visitor.getParameters();
            int maxOrdinal = 0;
            for (Map.Entry<Integer, Object> entry : conditionParamMap.entrySet()) {
                Integer ordinal = entry.getKey();
                paramMap.put(ordinal + offset, entry.getValue());
                if (ordinal > maxOrdinal) {
                    maxOrdinal = ordinal;
                }
            }
            offset = maxOrdinal;
        }
        if (compiledSelectionList.length() > 0) {
            compiledSelectionList.setLength(compiledSelectionList.length() - 2);
        }
        return new InfluxDBCompiledCondition(compiledSelectionList.toString(), paramMap, paramCons);
    }

    private InfluxDBCompiledCondition compileClause(List<ExpressionBuilder> expressionBuilders) {

        StringBuilder compiledSelectionList = new StringBuilder();
        SortedMap<Integer, Object> paramMap = new TreeMap<>();
        SortedMap<Integer, Object> paramCons = new TreeMap<>();
        int offset = 0;
        for (ExpressionBuilder expressionBuilder : expressionBuilders) {
            InfluxDBConditionVisitor visitor = new InfluxDBConditionVisitor();
            expressionBuilder.build(visitor);
            String compiledCondition = visitor.returnCondition();
            compiledSelectionList.append(compiledCondition).append(SEPARATOR);
            Map<Integer, Object> conditionParamMap = visitor.getParameters();
            Map<Integer, Object> conditionConsMap = visitor.getParametersConstant();
            int maxOrdinal = 0;
            for (Map.Entry<Integer, Object> entry : conditionParamMap.entrySet()) {
                Integer ordinal = entry.getKey();
                paramMap.put(ordinal + offset, entry.getValue());
                if (ordinal > maxOrdinal) {
                    maxOrdinal = ordinal;
                }
            }
            for (Map.Entry<Integer, Object> entry : conditionConsMap.entrySet()) {
                Integer ordinal = entry.getKey();
                paramCons.put(ordinal + offset, entry.getValue());
                if (ordinal > maxOrdinal) {
                    maxOrdinal = ordinal;
                }
            }
            offset = maxOrdinal;
        }
        if (compiledSelectionList.length() > 0) {
            compiledSelectionList.setLength(compiledSelectionList.length() - 2); // Removing the last comma separator.
        }
        return new InfluxDBCompiledCondition(compiledSelectionList.toString(), paramMap, paramCons);
    }

    private InfluxDBCompiledCondition compileOrderByClause(List<OrderByAttributeBuilder> orderByAttributeBuilders) {

        StringBuilder compiledSelectionList = new StringBuilder();
        SortedMap<Integer, Object> paramMap = new TreeMap<>();
        int offset = 0;
        for (OrderByAttributeBuilder orderByAttributeBuilder : orderByAttributeBuilders) {
            InfluxDBConditionVisitor visitor = new InfluxDBConditionVisitor();
            orderByAttributeBuilder.getExpressionBuilder().build(visitor);
            String compiledCondition = visitor.returnCondition();
            compiledSelectionList.append(compiledCondition);
            OrderByAttribute.Order order = orderByAttributeBuilder.getOrder();
            if (order == null) {
                compiledSelectionList.append(SEPARATOR);
            } else {
                compiledSelectionList.append(WHITESPACE).append(order.toString()).append(SEPARATOR);
            }
            Map<Integer, Object> conditionParamMap = visitor.getParameters();
            int maxOrdinal = 0;
            for (Map.Entry<Integer, Object> entry : conditionParamMap.entrySet()) {
                Integer ordinal = entry.getKey();
                paramMap.put(ordinal + offset, entry.getValue());
                if (ordinal > maxOrdinal) {
                    maxOrdinal = ordinal;
                }
            }
            offset = maxOrdinal;
        }
        if (compiledSelectionList.length() > 0) {
            compiledSelectionList.setLength(compiledSelectionList.length() - 2);
        }
        return new InfluxDBCompiledCondition(compiledSelectionList.toString(), paramMap, null);
    }
}

