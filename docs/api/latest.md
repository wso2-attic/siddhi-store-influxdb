# API Docs - v1.0.2-SNAPSHOT

## Store

### influxdb *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#store">(Store)</a>*

<p style="word-wrap: break-word">This extension connects to the influxDB store. It also implements read-write operations on the connected influxDB database.</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@Store(type="influxdb", url="<STRING>", username="<STRING>", password="<STRING>", influxdb.database="<STRING>", retention.policy="<STRING>", table.name="<STRING>")
@PrimaryKey("PRIMARY_KEY")
@Index("INDEX")
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">url</td>
        <td style="vertical-align: top; word-wrap: break-word"> The URL via which the InfluxDB data store is accessed.</td>
        <td style="vertical-align: top">http://localhost:8086 </td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">username</td>
        <td style="vertical-align: top; word-wrap: break-word"> The username used to access the InfluxDB data store.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">password</td>
        <td style="vertical-align: top; word-wrap: break-word"> The password used to access the InfluxDB data store </td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">influxdb.database</td>
        <td style="vertical-align: top; word-wrap: break-word"> The name of the InfluxDB database in which the data must be entered. </td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">retention.policy</td>
        <td style="vertical-align: top; word-wrap: break-word">This describes how long the InfluxDB retains the data.</td>
        <td style="vertical-align: top">autogen</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">table.name</td>
        <td style="vertical-align: top; word-wrap: break-word">The name with which the Siddhi store must be persisted in the InfluxDB database. If no name is specified via this parameter, the store is persisted in the InfluxDB database with the same name defined in the table definition of the Siddhi application.</td>
        <td style="vertical-align: top">The table name defined in the Siddhi Application query.</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
 @Store (type = "influxdb",
   url = "http://localhost:8086",
   username = "root",
   password = "root" ,
   database ="aTimeSeries")
@Index ("symbol","time")
define table StockTable (symbol string,volume long,price float,time long) ;
define stream StockStream (symbol string,volume long,price float);@info (name='query2') from StockStream
select symbol,price,volume,currentTimeMillis() as time
insert into StockTable ;
```
<p style="word-wrap: break-word"> This query creates a table in the database if it does not already exist with the following attributes:<br>- 'symbol' attribute as the tag key. This must be  included within the @Index annotation<br>- 'volume' and 'price' attributes as field keys.<br>- 'time'attribute as the time stamp associated with particular data. Here, the time does not need to be provided by you. It can be extracted from the current time of the Siddhi application.</p>

<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
 @Store (type = "influxdb",
   url = "http://localhost:8086",
   username = "root",
   password = "root" ,
   database ="aTimeSeries")
@Index ("symbol","time")
define table StockTable (symbol string,volume long,price float,time long) ;
define stream StockStream (symbol string,volume long,price float,time long);
@info (name = 'query1')  
from StockStream 
select symbol,  volume, price, time
insert into StockTable ;
```
<p style="word-wrap: break-word">The above query creates a table in the database if it does not already exist with the following:<br>- 'symbol' attribute as the tag key. This must be  included within the @Index annotation.<br>- 'volume' and 'price' attributes as field keys.<br>- 'time' attribute as the time stamp associated with particular data.</p>

<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
 @Store (type = "influxdb",
   url = "http://localhost:8086",
   username = "root",
   password = "root" ,
   database ="aTimeSeries")
@Index ("symbol","time")
define table StockTable(symbol string,volume long,price float,time long) ;
@info (name = 'query4')
from ReadStream#window.length(1) join StockTable on StockTable.symbol==ReadStream.symbols 
select StockTable.symbol as checkName, StockTable.price as checkCategory,
StockTable.volume as checkVolume,StockTable.time as checkTime
insert into OutputStream; 
```
<p style="word-wrap: break-word">The above query creates a table in the database if it does not already exist with the following:<br>- 'symbol' attribute as the tag key. This must be  included within the @Index annotation.<br>- 'volume' and 'price' attributes as field keys.<br>- 'time' attribute to represent the time stamp.<br>Once the table is created, it is joined with the stream named 'ReadStream' based on a condition.</p>

