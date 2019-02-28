# API Docs - v1.0.0-SNAPSHOT

## Store

### influxdb *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#store">(Store)</a>*

<p style="word-wrap: break-word">This extension assigns connection instructions to configure influxDB store. It also implements read-write operations on connected influxDB database.</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@Store(type="influxdb", url="<STRING>", username="<STRING>", password="<STRING>", database="<STRING>", table.name="<STRING>")
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
        <td style="vertical-align: top; word-wrap: break-word"> Url via which the InfluxDB data store is accessed</td>
        <td style="vertical-align: top">http://localhost:8086 </td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">username</td>
        <td style="vertical-align: top; word-wrap: break-word"> The username used to access InfluxDB data store </td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">password</td>
        <td style="vertical-align: top; word-wrap: break-word"> The password used to access InfluxDB data store </td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">database</td>
        <td style="vertical-align: top; word-wrap: break-word"> The database to which the data should be entered. </td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">table.name</td>
        <td style="vertical-align: top; word-wrap: break-word">The name with which the event table should be persisted in the store. If no name is specified via this parameter, the event table is persisted with the same name as the Siddhi table.</td>
        <td style="vertical-align: top">The table name defined in the Siddhi App query.</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
 @Store(type = "influxdb",
   url = "http://localhost:8086",
   username = "root",
   password = "root" ,
   database ="aTimeSeries")
@Index("symbol","time")
define table StockTable(symbol string,volume long,price float,time long) ;
define stream StockStream (symbol string,volume long,price float);@info(name='query2') from StockStream
select symbol,price,volume,currentTimeMillis() as time
insert into StockTable ;
```
<p style="word-wrap: break-word"> The above example creates a table in the  database if it does not exist with symbol which is configured under '@Index annotation as a tag key, 'volume ' and 'price' as field keys and time as the timestamp associated with particular data.Here the time is not needed to be given by the user and it extract the current time of the siddhi application.</p>

<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
 @Store(type = "influxdb",
   url = "http://localhost:8086",
   username = "root",
   password = "root" ,
   database ="aTimeSeries")
@Index("symbol","time")
define table StockTable(symbol string,volume long,price float,time long) ;
define stream StockStream (symbol string,volume long,price float,time long);
@info(name = 'query1')  
from StockStream 
select symbol,  volume, price, time
insert into StockTable ;
```
<p style="word-wrap: break-word"> The above example creates a table in the  database if it does not exist already with 'symbol which is configured under '@Index' annotation  as a tag key and 'volume' and 'price' as field keys . Time represents the timestamp  associated with a particular data.</p>

<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
 @Store(type = "influxdb",
   url = "http://localhost:8086",
   username = "root",
   password = "root" ,
   database ="aTimeSeries")
@Index("symbol","time")
define table StockTable(symbol string,volume long,price float,time long) ;
@info(name = 'query4')
from ReadStream#window.length(1) join StockTable on StockTable.symbol==ReadStream.symbols 
select StockTable.symbol as checkName, StockTable.price as checkCategory,
StockTable.volume as checkVolume,StockTable.time as checkTime
insert into OutputStream; 
```
<p style="word-wrap: break-word"> The above example creates a table in the database if it does not exist already with 'symbol' which is configured under '@Index' annotation  as a tag key , 'volume' and 'price' as field keys and time represents the timestampThen the table is joined with a stream named 'ReadStream' based on a condition.</p>

