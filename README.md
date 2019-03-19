siddhi-store-influxdb
======================================

The **siddhi-store-influxdb extension** is an extension to <a></a> that can be used to persist events to a InfluxDB instance of the users choice.
Find some useful links below.

* <a target= "_blank" href="https://github.com/wso2-extensions/siddhi-store-influxdb">Source code</a>
* <a target= "_blank" href="https://github.com/wso2-extensions/siddhi-store-influxdb/releases">Releases</a>
* <a target= "_blank" href="https://github.com/wso2-extensions/siddhi-store-influxdb/issues">Issue tracker</a>

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://wso2-extensions.github.io/siddhi-store-influxdb/api/1.0.1">1.0.1</a>.

## Prerequisites

* A InfluxDB server instance should be started.
* User should have the necessary privileges and access rights to connect to the InfluxDB data store of choice.
* You can start the InfluxDB server using docker image.

1.Start the container.

```
docker run --name=influxdb -d -p 8086:8086 influxdb
```

2.Run the influxDB client in this container.
```
docker exec -it influxdb influx
```
3.Create a database in influxDB using the command :
```
CREATE DATABASE <database_name>
```

## How to use

**Using the extension in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

* You can use this extension in the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support.

* This extension is shipped by default with WSO2 Stream Processor, if you wish to use an alternative version of this extension you can replace the component <a target="_blank" href="https://github.com/wso2-extensions/siddhi-store-influxdb/releases">jar</a> that can be found in the `<STREAM_PROCESSOR_HOME>/lib` directory.

**Using the extension as a <a target="_blank" href="https://wso2.github.io/siddhi/documentation/running-as-a-java-library">java library</a>**

* This extension can be added as a maven dependency along with other Siddhi dependencies to your project.

```
     <dependency>
        <groupId>org.wso2.extension.siddhi.store.influxdb</groupId>
        <artifactId>siddhi-store-influxdb</artifactId>
        <version>x.x.x</version>
     </dependency>
```
## Jenkins Build Status

---

|  Branch | Build Status |
| :------ |:------------ | 
| master  | [![Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-store-influxdb/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-store-influxdb/) |

---
## Features

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-store-influxdb/api/1.0.1/#influxdb-store">influxdb</a> *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#store">(Store)</a>*<br><div style="padding-left: 1em;"><p>This extension connects to  influxDB store. It also implements read-write operations on connected influxDB database.</p></div>

## Contact us

 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>.

 * Siddhi developers can be contacted via the mailing lists:

    Developers List   : [dev@wso2.org](mailto:dev@wso2.org)

    Architecture List : [architecture@wso2.org](mailto:architecture@wso2.org)

## Support

* We are committed to ensuring support for this extension in production. Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology.

* For more details and to take advantage of this unique opportunity contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>.
