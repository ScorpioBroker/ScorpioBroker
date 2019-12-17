# Scorpio NGSI-LD Broker

[![FIWARE Core](https://nexus.lab.fiware.org/static/badges/chapters/core.svg)](https://www.fiware.org/developers/catalogue/)
[![License: BSD-4-Clause](https://img.shields.io/badge/license-BSD%204%20Clause-blue.svg)](https://spdx.org/licenses/BSD-4-Clause.html)
[![Docker](https://img.shields.io/docker/pulls/scorpiobroker/scorpio.svg)](https://hub.docker.com/r/scorpiobroker/scorpio/)
[![fiware](https://nexus.lab.fiware.org/repository/raw/public/badges/stackoverflow/fiware.svg)](https://stackoverflow.com/questions/tagged/fiware)
<br>
[![Documentation badge](https://img.shields.io/readthedocs/scorpio.svg)](https://scorpio.readthedocs.io/en/latest/?badge=latest)
![Status](https://nexus.lab.fiware.org/static/badges/statuses/incubating.svg)

Scorpio is an NGSI-LD compliant context broker developed by NEC Laboratories Europe and NEC Technologies India.

This project is part of [FIWARE](https://www.fiware.org/). For more information check the FIWARE Catalogue entry for
[Core Context](https://github.com/Fiware/catalogue/tree/master/core).

| :books: [Documentation](https://scorpio.rtfd.io/) | :whale: [Docker Hub](https://hub.docker.com/r/scorpiobroker/scorpio/) |
| ------------------------------------------------- | --------------------------------------------------------------------- |


## Content

-   [Background](#background)
-   [Installation](#installation)
-   [Usage](#usage)
-   [License](#license)

## Background

NGSI-LD is an open API and Datamodel specification for context management
[published by ETSI](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.01.01_60/gs_CIM009v010101p.pdf).

## Installation

Scorpio is developed in Java using SpringCloud as microservice framework and Apache Maven as build tool. Some of the
tests require a running Apache Kafka messagebus (further instruction are in the Setup chapter). If you want to skip
those tests you can run `mvn clean package -DskipTests` to just build the individual microservices.





### General Remarks on building
Further down this document you will get exact build commands/arguments for the different flavors. This part will give you an overview on how the different arguments
#### Maven Profiles
There currently three available Maven build profiles 
##### Default 
If you provide no -P argument Maven will produce individual jar files for the microservices and the AllInOneRunner with each "full" microservice packaged (this will result in ca. 500 MB size for the AllInOneRunner)

##### docker
This will trigger the Maven to build docker containers for each microservice.  

##### docker-aaio
This will trigger the Maven to build one docker container, containing the AllInOneRunner and the spring cloud components (eureka, configserver and gateway)  
#### Maven arguments
These arguments are provided via -D in the command line.
##### skipTests
Generally recommended if you want to speed up the build or you don't have a kafka instance running, which is required by some of the tests.
##### skipDefault
This is a special argument for the Scorpio build. This argument will disable springs repacking for the individual microservices and will allow for a smaller AllInOneRunner jar file. This argument shoulnd ONLY be used in combination with the docker-aaio profile.

#### Spring Profiles
Spring supports also profiles which can be activated when launching a jar file. Currently there 3 profiles actively used in Scorpio.
The default profiles assume the default setup to be a individual microservices. The exception is the AllInOneRunner which as default assumes to be running in the docker-aaio setup.

Currently you should be able to run everything with a default profile except the gateway in combination with the AllInOneRunner. 
In order to use these two together you need to start the gateway with the aaio spring profile. This can be done by attaching this to your start command -Dspring.profiles.active=aaio.

Additonally some components have a dev profile available which is purely meant for development purposes and should only be used for such. 
  
### Setup
Scorpio requires two components to be installed.

#### Postgres

Please download the [Postgres DB](https://www.postgresql.org/) and the [Postgis](https://postgis.net) extension and
follow the instructions on the websites to set them up.

Scorpio has been tested and developed with Postgres 10.

The default username and password which Scorpio uses is "ngb". If you want to use a different username or password you
need to provide them as parameter when starting the StorageManager and the RegistryManager.

e.g.

```console
java -jar Storage/StorageManager/target/StorageManager-<VERSIONNUMBER>-SNAPSHOT.jar --reader.datasource.username=funkyusername --reader.datasource.password=funkypassword
```

OR

```console
java -jar Registry/RegistryManager/target/RegistryManager-<VERSIONNUMBER>-SNAPSHOT.jar --spring.datasource.username=funkyusername --spring.datasource.password=funkypassword
```

Don't forget to create the corresponding user ("ngb" or the different username you chose) in postgres. It will be used
by the SpringCloud services for database connection. While in terminal, log in to the psql console as postgres user:

```console
sudo -u postgres psql
```

Then create a database "ngb":

```console
postgres=# create database ngb;
```

Create a user "ngb" and make him a superuser:

```console
postgres=# create user ngb with encrypted password 'ngb';
postgres=# alter user ngb with superuser;
```

Grant privileges on database:

```console
postgres=# grant all privileges on database ngb to ngb;
```

Also create an own database/schema for the Postgis extension:

```console
postgres=# CREATE DATABASE gisdb;
postgres=# \connect gisdb;
postgres=# CREATE SCHEMA postgis;
postgres=# ALTER DATABASE gisdb SET search_path=public, postgis, contrib;
postgres=# \connect gisdb;
postgres=# CREATE EXTENSION postgis SCHEMA postgis;
```

#### Apache Kafka

Scorpio uses [Apache Kafka](https://kafka.apache.org/) for the communication between the microservices.

Scorpio has been tested and developed with Kafka version 2.12-2.1.0

Please download [Apache Kafka](https://kafka.apache.org/downloads) and follow the instructions on the website.

In order to start kafka you need to start two components:<br> Start zookeeper with

```console
<kafkafolder>/bin/[Windows]/zookeeper-server-start.[bat|sh] <kafkafolder>/config/zookeeper.properties
```

Start kafkaserver with

```console
<kafkafolder>/bin/[Windows]/kafka-server-start.[bat|sh] <kafkafolder>/config/server.properties
```

For more details please visit the Kafka [website](https://kafka.apache.org/).

### Getting a docker container

The current maven build supports two types of docker container generations from the build using maven profiles to
trigger it.

The first profile is called 'docker' and can be called like this

```console
sudo mvn clean package -DskipTests -Pdocker
```

this will generate individual docker containers for each micro service. The corresponding docker-compose file is
`docker-compose-dist.yml`

The second profile is called 'docker-aaio' (for almost all in one). This will generate one single docker container for
all components the broker except the kafka message bus and the postgres database.

To get the aaio version run the maven build like this

```console
sudo mvn clean package -DskipTests -DskipDefault -Pdocker-aaio
```

The corresponding docker-compose file is `docker-compose-aaio.yml`

#### General remark for the Kafka docker image and docker-compose

The Kafka docker container requires you to provide the environment variable `KAFKA_ADVERTISED_HOST_NAME`. This has to be
changed in the docker-compose files to match your docker host IP. You can use `127.0.0.1` however this will disallow you
to run Kafka in a cluster mode.

For further details please refer to https://hub.docker.com/r/wurstmeister/kafka

#### Running docker build outside of Maven

If you want to have the build of the jars separated from the docker build you need to provide certain VARS to docker.
The following list shows all the vars and their intended value if you run docker build from the root dir

-   `BUILD_DIR_ACS = Core/AtContextServer`

-   `BUILD_DIR_SCS = SpringCloudModules/config-server`

-   `BUILD_DIR_SES = SpringCloudModules/eureka`

-   `BUILD_DIR_SGW = SpringCloudModules/gateway`

-   `BUILD_DIR_HMG = History/HistoryManager`

-   `BUILD_DIR_QMG = Core/QueryManager`

-   `BUILD_DIR_RMG = Registry/RegistryManager`

-   `BUILD_DIR_EMG = Core/EntityManager`

-   `BUILD_DIR_STRMG = Storage/StorageManager`

-   `BUILD_DIR_SUBMG = Core/SubscriptionManager`

-   `JAR_FILE_BUILD_ACS = AtContextServer-${project.version}.jar`

-   `JAR_FILE_BUILD_SCS = config-server-${project.version}.jar`

-   `JAR_FILE_BUILD_SES = eureka-server-${project.version}.jar`

-   `JAR_FILE_BUILD_SGW = gateway-${project.version}.jar`

-   `JAR_FILE_BUILD_HMG = HistoryManager-${project.version}.jar`

-   `JAR_FILE_BUILD_QMG = QueryManager-${project.version}.jar`

-   `JAR_FILE_BUILD_RMG = RegistryManager-${project.version}.jar`

-   `JAR_FILE_BUILD_EMG = EntityManager-${project.version}.jar`

-   `JAR_FILE_BUILD_STRMG = StorageManager-${project.version}.jar`

-   `JAR_FILE_BUILD_SUBMG = SubscriptionManager-${project.version}.jar`

-   `JAR_FILE_RUN_ACS = AtContextServer.jar`

-   `JAR_FILE_RUN_SCS = config-server.jar`

-   `JAR_FILE_RUN_SES = eureka-server.jar`

-   `JAR_FILE_RUN_SGW = gateway.jar`

-   `JAR_FILE_RUN_HMG = HistoryManager.jar`

-   `JAR_FILE_RUN_QMG = QueryManager.jar`

-   `JAR_FILE_RUN_RMG = RegistryManager.jar`

-   `JAR_FILE_RUN_EMG = EntityManager.jar`

-   `JAR_FILE_RUN_STRMG = StorageManager.jar`

-   `JAR_FILE_RUN_SUBMG = SubscriptionManager.jar`

### Starting of the components

After the build start the individual components as normal Jar files.

Start the SpringCloud services by running

```console
java -jar SpringCloudModules/eureka/target/eureka-server-<VERSIONNUMBER>-SNAPSHOT.jar
java -jar SpringCloudModules/gateway/target/gateway-<VERSIONNUMBER>-SNAPSHOT.jar
java -jar SpringCloudModules/config-server/target/config-server-<VERSIONNUMBER>-SNAPSHOT.jar
```

Start the broker components

```console
java -jar Storage/StorageManager/target/StorageManager-<VERSIONNUMBER>-SNAPSHOT.jar
java -jar Core/QueryManager/target/QueryManager-<VERSIONNUMBER>-SNAPSHOT.jar
java -jar Registry/RegistryManager/target/RegistryManager-<VERSIONNUMBER>-SNAPSHOT.jar
java -jar Core/EntityManager/target/EntityManager-<VERSIONNUMBER>-SNAPSHOT.jar
java -jar History/HistoryManager/target/HistoryManager-<VERSIONNUMBER>-SNAPSHOT.jar
java -jar Core/SubscriptionManager/target/SubscriptionManager-<VERSIONNUMBER>-SNAPSHOT.jar
java -jar Core/AtContextServer/target/AtContextServer-<VERSIONNUMBER>-SNAPSHOT.jar
```

#### Changing config

All configurable options are present in application.properties files. In order to change those you have two options.
Either change the properties before the build or you can override configs by add `--<OPTION_NAME>=<OPTION_VALUE)` e.g.

```console
java -jar Storage/StorageManager/target/StorageManager-<VERSIONNUMBER>-SNAPSHOT.jar --reader.datasource.username=funkyusername --reader.datasource.password=funkypassword`
```

## Usage

By default the broker runs on port 9090 the base URL for interaction with the broker would be than
http://localhost:9090/ngsi-ld/v1/ For a detail explaination about the API please look the ETSI spec.

Generally speaking you can Create entities by sending an HTTP POST request to http://localhost:9090/ngsi-ld/v1/entities
with a payload like this

```json
{
    "id": "urn:ngsi-ld:testunit:123",
    "type": "AirQualityObserved",
    "dateObserved": {
        "type": "Property",
        "value": {
            "@type": "DateTime",
            "@value": "2018-08-07T12:00:00Z"
        }
    },
    "NO2": {
        "type": "Property",
        "value": 22,
        "unitCode": "GP",
        "accuracy": {
            "type": "Property",
            "value": 0.95
        }
    },
    "refPointOfInterest": {
        "type": "Relationship",
        "object": "urn:ngsi-ld:PointOfInterest:RZ:MainSquare"
    },
    "@context": [
        "https://schema.lab.fiware.org/ld/context",
        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
    ]
}
```

In the given example the `@context` is in the payload therefore you have to set the `ContentType` header to
`application/ld+json`

To receive entities you can send an HTTP GET to

`http://localhost:9090/ngsi-ld/v1/entities/<entityId>`

or run a query by sending a GET like this

```text
http://localhost:9090/ngsi-ld/v1/entities/?type=Vehicle&limit=2
Accept: application/ld+json
Link: <http://<HOSTNAME_OF_WHERE_YOU_HAVE_AN_ATCONTEXT>/aggregatedContext.jsonld>; rel="http://www.w3.org/ns/json-ld#context";type="application/ld+json"
```

For more detailed explaination on NGSI-LD or JSON-LD. Please look at the
[ETSI Specification](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.01.01_60/gs_CIM009v010101p.pdf) or visit
the [JSON-LD website](https://json-ld.org/).

### Enable CORS support
You can enable cors support in the gateway by providing these configuration options
 - gateway.enablecors  -   default is False. Set to true for general enabling
 - gateway.enablecors.allowall  -   default is False. Set to true to enable CORS from all origins, allow all headers and all methods. Not secure but still very often used.
 - gateway.enablecors.allowedorigin  -   A comma separated list of allowed origins
 - gateway.enablecors.allowedheader  -   A comma separated list of allowed headers
 - gateway.enablecors.allowedmethods  -   A comma separated list of allowed methods 
 - gateway.enablecors.allowallmethods  -   default is False. Set to true to allow all methods. If set to true it will override the allowmethods entry


### Postman example collection

You can find a set of example calls, as a Postman collection, in the Examples folder. These examples use 2 Variables

- gatewayServer, which has to be `<brokerIP>:<brokerPort>`. When using default settings locally it would be localhost:9090
- link, which is for the examples providing @context via the Link header. For the examples we host an example @context. Set link to https://raw.githubusercontent.com/ScorpioBroker/ScorpioBroker/master/Examples/index.json

## Troubleshooting

### Missing JAXB dependencies

When starting the eureka-server you may facing the **java.lang.TypeNotPresentException: Type javax.xml.bind.JAXBContext
not present** exception. It's very likely that you are running Java 11 on your machine then. Starting from Java 9
package `javax.xml.bind` has been marked deprecated and was finally completely removed in Java 11.

In order to fix this issue and get eureka-server running you need to manually add below JAXB Maven dependencies to
`ScorpioBroker/SpringCloudModules/eureka/pom.xml` before starting:

```xml
...
<dependencies>
        ...
        <dependency>
                <groupId>com.sun.xml.bind</groupId>
                <artifactId>jaxb-core</artifactId>
                <version>2.3.0.1</version>
        </dependency>
        <dependency>
                <groupId>javax.xml.bind</groupId>
                <artifactId>jaxb-api</artifactId>
                <version>2.3.1</version>
        </dependency>
        <dependency>
                <groupId>com.sun.xml.bind</groupId>
                <artifactId>jaxb-impl</artifactId>
                <version>2.3.1</version>
        </dependency>
        ...
</dependencies>
...
```
This should be fixed now using conditional dependencies. 
## Acknowledgements
Part of the development has been founded by the EU in the AUTOPILOT project.

### EU Acknowledgetment
This activity has received funding from the European Union’s Horizon 2020 research and innovation programme under Grant Agreement No 731993. <img src="https://raw.githubusercontent.com/ScorpioBroker/ScorpioBroker/master/img/flag_yellow_low.jpg" width="160">

### Autopilot
Part of the development was done in and for the [AUTOPILOT project for Automated driving Progressed by Internet Of Things](https://autopilot-project.eu/) <img src="https://raw.githubusercontent.com/ScorpioBroker/ScorpioBroker/master/img/autopilot.png" width="160">

## License

Scorpio is licensed under [BSD-4-Clause](https://spdx.org/licenses/BSD-4-Clause.html).

© 2019 NEC
