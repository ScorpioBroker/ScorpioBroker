# Scorpio NGSI-LD Broker

Scorpio is an NGSI-LD compliant context broker developed by NEC Laboratories Europe and NEC Technologies India.

## NGSI-LD

NGSI-LD is an open API and Datamodel specification for context management [published by ETSI](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.01.01_60/gs_CIM009v010101p.pdf).

## Building

Scorpio is developed in Java using SpringCloud as microservice framework and Apache Maven as build tool. 
Some of the tests require a running Apache Kafka messagebus (further instruction are in the Setup chapter). If you want to skip those tests you can run "mvn clean package -DskipTests" to just build the individual microservices.

## Setup 
Scorpio requires two components to be installed.

### Postgres

Please download the [Postgres DB](https://www.postgresql.org/) and the [Postgis](https://postgis.net) extension and follow the instructions on the websites to set them up.

Scorpio has been tested and developed with Postgres 10. 

The default username and password which Scorpio uses is "ngb". If you want to use a different username or password you need to provide them as parameter when starting the StorageManager and the RegistryManager.

e.g. java -jar StorageManager-<VERSIONNUMBER>-SNAPSHOT.jar --reader.datasource.username=funkyusername --reader.datasource.password=funkypassword
or 
java -jar RegistryManager-<VERSIONNUMBER>-SNAPSHOT.jar --spring.datasource.username=funkyusername --spring.datasource.password=funkypassword

### Apache Kafka

Scorpio uses [Apache Kafka](https://kafka.apache.org/) for the communication between the microservices.

Scorpio has been tested and developed with Kafka version 2.12-2.1.0

Please download [Apache Kafka](https://kafka.apache.org/downloads) and follow the instructions on the website. 

In order to start kafka you need to start two components 
Start zookeeper with <kafkafolder>/bin/[Windows]/zookeeper-server-start.[bat|sh] [..]/../config/zookeeper.properties
Start kafkaserver with <kafkafolder>/bin/[Windows]/kafka-server-start.[bat|sh] [..]/../config/server.properties

For more details please visit the Kafka website.

## Starting of the components

After the build start the individual components as normal Jar files.

Start the SpringCloud services by running 

java -jar eureka-server-<VERSIONNUMBER>-SNAPSHOT.jar

java -jar gateway-<VERSIONNUMBER>-SNAPSHOT.jar

java -jar config-server-<VERSIONNUMBER>-SNAPSHOT.jar


Start the broker components 

java -jar StorageManager-<VERSIONNUMBER>-SNAPSHOT.jar

java -jar QueryManager-<VERSIONNUMBER>-SNAPSHOT.jar

java -jar RegistryManager-<VERSIONNUMBER>-SNAPSHOT.jar

java -jar EntityManager-<VERSIONNUMBER>-SNAPSHOT.jar

java -jar HistoryManager-<VERSIONNUMBER>-SNAPSHOT.jar

java -jar SubscriptionManager-<VERSIONNUMBER>-SNAPSHOT.jar

java -jar AtContextServer-<VERSIONNUMBER>-SNAPSHOT.jar

### Changing config 
All configurable options are present in application.properties files. In order to change those you have two options.
Either change the properties before the build or you can override configs by add --<OPTION_NAME>=<OPTION_VALUE)
e.g. 

java -jar StorageManager.jar --reader.datasource.username=funkyusername --reader.datasource.password=funkypassword

## Basic interaction

By default the broker runs on port 9090 the base URL for interaction with the broker would be than
http://localhost:9090/ngsi-ld/v1/
For a detail explaination about the API please look the ETSI spec.


Generally speaking you can 
Create entities by sending an HTTP POST request to http://localhost:9090/ngsi-ld/v1/entities
with a payload like this 

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


In the given example the @context is in the payload therefor you have to set the ContentType header to application/ld+json

To receive entities you can send an HTTP GET to 

http://localhost:9090/ngsi-ld/v1/entities/<entityId>

or run a query by sending a GET like this 

http://localhost:9090/ngsi-ld/v1/entities/?type=Vehicle&limit=2 
Accept: application/ld+json 
Link: <http://<HOSTNAME_OF_WHERE_YOU_HAVE_AN_ATCONTEXT>/aggregatedContext.jsonld>; rel="http://www.w3.org/ns/json-ld#context";type="application/ld+json"

For more detailed explaination on NGSI-LD or JSON-LD. Please look at the [ETSI Specification](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.01.01_60/gs_CIM009v010101p.pdf) or visit the [JSON-LD website](https://json-ld.org/).




