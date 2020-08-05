[![FIWARE Core Context Management](https://nexus.lab.fiware.org/repository/raw/public/badges/chapters/core.svg)](https://github.com/FIWARE/catalogue/blob/master/core/README.md)
[![NGSI v2](https://img.shields.io/badge/NGSI-v2-5dc0cf.svg)](link)

**Description:** This is an Introductory Tutorial to the FIWARE Platform. We will start with the data from a different houses of a smart city
 and create a very simple _“Powered by FIWARE”_ application by passing in the temperature of various rooms of that house as context data to the FIWARE context broker.

The tutorial uses [cUrl](https://ec.haxx.se/) commands throughout, but is also available as
[Postman documentation](link)

[![Run in Postman](https://run.pstmn.io/button.svg)]( postman link)

<hr class="core"/>

# Architecture

Our demo application will only make use of one FIWARE component - the
[Scorpio Broker](link to docs). Usage of the Scorpio Context Broker (with proper
context data flowing through it) is sufficient for an application to qualify as _“Powered by FIWARE”_.

![Deployment Architecture](img/deploymentarchitecture.png)

The deployment architecture leverages the Spring Cloud framework that addresses lots of Micro-services concerns (e.g. scaling, monitoring, fault-tolerant, highly available, secure, decoupled, etc. ) and Kafka based distributed and scalable message queue infrastructure to provide high performance on message processing for a huge number of context requests which is usual in the IoT domain. 
The deployment architecture covers the high-level operations (Http based REST with method POST/GET/DELETE/PATCH) request flow from the external world to the Scorpio Broker system.  The external request is served through a unified service API gateway interface that exposes a single IP/port combination to be used for all services that the Scorpio Broker system can provide. 
In reality, each of the Scorpio Broker services will be implemented as a micro-service that can be deployed as an independent standalone unit in adistributed computing environment.
That API gateway routes all the incoming requests to the specific Micro-services with the help of registration & discovery service. 
Once the request reaches at micro-service based on the operation requirement it uses(pub/sub) Kafka topics (message queues)  for real-time storage and for providing intercommunication among different micro-services (based on requirement) over message queues.

## Prerequisites

### Docker

Docker is a tool designed to make it easier to create, deploy, and run applications by using containers.
Containers allow a developer to package up an application with all of the parts it needs, such as libraries and other dependencies, and deploy it as one package

-   To get Docker on Windows, click [here](https://docs.docker.com/docker-for-windows/)
-   To get Docker on Mac, click [here](https://docs.docker.com/docker-for-mac/)
-   To get Docker on Linux, click [here](https://docs.docker.com/install/)

### Getting a docker container

The current maven build supports two types of docker container
generations from the build using maven profiles to trigger it.

The first profile is called 'docker' and can be called like this 
```bash
mvn clean package -DskipTests -Pdocker
```
this will generate individual docker containers for each microservice.
The corresponding docker-compose file is docker-compose-dist.yml

The second profile is called 'docker-aaio' (for almost all in one). This
will generate one single docker container for all components of the
broker except the Kafka message bus and the Postgres database.

To get the aaio version run the maven build like this
```bash
mvn clean package -DskipTests -Pdocker-aaio
```
The corresponding docker-compose file is docker-compose-aaio.yml

### General remark for the Kafka docker image and docker-compose

The Kafka docker container requires you to provide the environment
variable KAFKA\_ADVERTISED\_HOST\_NAME. This has to be changed in the
docker-compose files to match your docker host IP. You can use 127.0.0.1
however this will disallow you to run Kafka in a cluster mode.

For further details please refer to
<https://hub.docker.com/r/wurstmeister/kafka>

### Running docker build outside of Maven

If you want to have the build of the jars separated from the docker
build you need to provide certain VARS to docker. The following list
shows all the vars and their intended value if you run docker build from
the root dir

```bash
- BUILD\_DIR\_ACS = Core/AtContextServer 
- BUILD\_DIR\_SCS = SpringCloudModules/config-server 
- BUILD\_DIR\_SES =SpringCloudModules/eureka 
- BUILD\_DIR\_SGW =SpringCloudModules/gateway 
- BUILD\_DIR\_HMG = History/HistoryManager
- BUILD\_DIR\_QMG = Core/QueryManager 
- BUILD\_DIR\_RMG =Registry/RegistryManager 
- BUILD\_DIR\_EMG = Core/EntityManager 
- BUILD\_DIR\_STRMG = Storage/StorageManager 
- BUILD\_DIR\_SUBMG =Core/SubscriptionManager
- JAR\_FILE\_BUILD\_ACS = AtContextServer-\${project.version}.jar 
- JAR\_FILE\_BUILD\_SCS = config-server-\${project.version}.jar 
- JAR\_FILE\_BUILD\_SES = eureka-server-\${project.version}.jar 
- JAR\_FILE\_BUILD\_SGW = gateway-\${project.version}.jar 
- JAR\_FILE\_BUILD\_HMG = HistoryManager-\${project.version}.jar 
- JAR\_FILE\_BUILD\_QMG = QueryManager-\${project.version}.jar 
- JAR\_FILE\_BUILD\_RMG = RegistryManager-\${project.version}.jar
- JAR\_FILE\_BUILD\_EMG = EntityManager-\${project.version}.jar 
- JAR\_FILE\_BUILD\_STRMG = StorageManager-\${project.version}.jar 
- JAR\_FILE\_BUILD\_SUBMG = SubscriptionManager-\${project.version}.jar
- JAR\_FILE\_RUN\_ACS = AtContextServer.jar 
- JAR\_FILE\_RUN\_SCS =config-server.jar 
- JAR\_FILE\_RUN\_SES = eureka-server.jar 
- JAR\_FILE\_RUN\_SGW = gateway.jar 
- JAR\_FILE\_RUN\_HMG =HistoryManager.jar 
- JAR\_FILE\_RUN\_QMG = QueryManager.jar 
- JAR\_FILE\_RUN\_RMG = RegistryManager.jar 
- JAR\_FILE\_RUN\_EMG =EntityManager.jar 
- JAR\_FILE\_RUN\_STRMG = StorageManager.jar 
- JAR\_FILE\_RUN\_SUBMG = SubscriptionManager.jar
```

## Creating your first "Powered by FIWARE" app

### Checking the service health

You can check if the Scorpio Broker is running by making an HTTP request to the exposed port:

#### 1 Request:

```bash
curl -X GET \
  'http://localhost:1026/version'
```

#### Response:

The response will look similar to the following:

```json
add response.........
```

> **What if I get a `Failed to connect to localhost port 1026: Connection refused` Response?**
>
> If you get a `Connection refused` response, the Scorpio Broker cannot be found where expected for this
> tutorial - you will need to substitute the URL and port in each cUrl command with the corrected IP address. All the
> cUrl commands tutorial assume that Scorpio is available on `localhost:1026`.
>
> Try the following remedies:
>
> -   To check that the docker containers are running try the following:
>
> ```
> docker ps
> ```
>
> You should see two containers running. If  Scorpio is not running, you can restart the containers as necessary. This
> command will also display open port information.
>
> -   If you have installed [`docker-machine`](https://docs.docker.com/machine/) and
>     [Virtual Box](https://www.virtualbox.org/), the  Scorpio docker container may be running from another IP address -
>     you will need to retrieve the virtual host IP as shown:
>
> ```
> curl -X GET \
>  'http://$(docker-machine ip default):1026/version'
> ```
>
> Alternatively run all your cUrl commands from within the container network:
>
> ```
> docker run --network fiware_default --rm appropriate/curl -s \
>  -X GET 'http:// Scorpio:1026/version'
> ```

## Creating Context Data

At its heart, FIWARE is a system for managing context information, so lets add some context data into the system by
creating two new entities (houses in **Smart city**). Any entity must have a `id` and `type` attributes, additional
attributes are optional and will depend on the system being described. Each additional attribute should also have a
defined `type` and a `value` attribute.

#### 2 Request:

```bash
curl localhost:9090/ngsi-ld/v1/entities -s -S -H 'Content-Type: application/json' -d @-
{
  "id": "house2:smartrooms:room1",
  "type": "Room",
  "temperature": {
    "value": 23,
	"unitCode": "CEL"
    "type": "Property"
	"providedBy": {
		"type": "Relationship",
		"object": "smartbuilding:house2:sensor0815"
  },
  "isPartOf": {
    "type": "Relationship",
	"object": "smartcity:houses:house2"
  },
  "@context": [{"Room": "urn:mytypes:room", "temperature": "myuniqueuri:temperature", "isPartOf": "myuniqueuri:isPartOf"},"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"]
}
```

#### 3 Request:

Each subsequent entity must have a unique `id` for the given `type`

```bash
curl localhost:9090/ngsi-ld/v1/entities -s -S -H 'Content-Type: application/json' -d @- 
{
  "id": "house2:smartrooms:room2",
  "type": "Room",
  "temperature": {
    "value": 21,
	"unitCode": "CELCIUS" 
    "type": "Property"
	"providedBy": {
		"type": "Relationship",
		"object": "smartbuilding:house2:sensor4711"
  },
  "belongsTo": {
    "type": "Relationship",
	"object": "smartcity:houses:house2"
  },
  "@context": [{"Room": "urn:mytypes:room", "temperature": "myuniqueuri:temperature", "belongsTo": "myuniqueuri:belongsTo"},"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"]
}
```

### Data Model Guidelines

Although the each data entity within your context will vary according to your use case, the common structure within each
data entity should be standardized order to promote reuse. The full FIWARE data model guidelines can be found
[here](https://fiware-datamodels.readthedocs.io/en/latest/guidelines/index.html). This tutorial demonstrates the usage
of the following recommendations:

#### All terms are defined in American English

Although the `value` fields of the context data may be in any language, all attributes and types are written using the
English language.

#### Entity type names must start with a Capital letter

In this case we only have one entity type - **Room**

#### Entity IDs should be a URN following NGSI-LD guidelines

NGSI-LD has recently been published as a full ETSI
[specification](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.01.01_60/gs_CIM009v010101p.pdf), the proposal is
that each `id` is a URN follows a standard format: `urn:ngsi-ld:<entity-type>:<entity-id>`. This will mean that every
`id` in the system will be unique

#### Data type names should reuse schema.org data types where possible

[Schema.org](http://schema.org/) is an initiative to create common structured data schemas. In order to promote reuse we
have deliberately used the [`Text`](http://schema.org/PostalAddress) and
[`PostalAddress`](http://schema.org/PostalAddress) type names within our **Store** entity. Other existing standards such
as [Open311](http://www.open311.org/) (for civic issue tracking) or [Datex II](https://datex2.eu/) (for transport
systems) can also be used, but the point is to check for the existence of the same attribute on existing data models and
reuse it.

#### Use camel case syntax for attribute names

The `streetAddress`, `addressRegion`, `addressLocality` and `postalCode` are all examples of attributes using camel
casing

#### Location information should be defined using `address` and `location` attributes

-   We have used an `address` attribute for civic locations as per [schema.org](http://schema.org/)
-   We have used a `location` attribute for geographical coordinates.

#### Use GeoJSON for codifying geospatial properties

[GeoJSON](http://geojson.org) is an open standard format designed for representing simple geographical features. The
`location` attribute has been encoded as a geoJSON `Point` location.

### Attribute Metadata

Metadata is _"data about data"_, it is additional data to describe properties of the attribute value itself like
accuracy, provider, or a timestamp. Several built-in metadata attribute already exist and these names are reserved

-   `dateCreated` (type: DateTime): attribute creation date as an ISO 8601 string.
-   `dateModified` (type: DateTime): attribute modification date as an ISO 8601 string.
-   `previousValue` (type: any): only in notifications. The value of this
-   `actionType` (type: Text): only in notifications.

One element of metadata can be found within the `address` attribute. a `verified` flag indicates whether the address has
been confirmed.

## Querying Context Data

A consuming application can now request context data by making HTTP requests to the  Scorpio Context Broker. The existing
NGSI interface enables us to make complex queries and filter results.

At the moment, all the context data is being added directly via HTTP requests, however in a
more complex smart solution, the  Scorpio Context Broker will also retrieve context directly from attached sensors
associated to each entity.

Here are a few examples, in each case the `options=keyValues` query parameter has been used shorten the responses by
stripping out the type elements from each attribute

### Obtain entity data by ID

This example returns the data of `urn:ngsi-ld:Store:001`

#### 4 Request:

```bash
curl -G -X GET \
   'http://localhost:1026/v2/entities/urn:ngsi-ld:Store:001' \
   -d 'options=keyValues'
```

#### Response:

Because of the use of the `options=keyValues`, the response consists of JSON only without the attribute `type` and
`metadata` elements.

```json
{
    "id": "urn:ngsi-ld:Store:001",
    "type": "Store",
    "address": {
        "streetAddress": "Bornholmer Straße 65",
        "addressRegion": "Berlin",
        "addressLocality": "Prenzlauer Berg",
        "postalCode": "10439"
    },
    "location": {
        "type": "Point",
        "coordinates": [13.3986, 52.5547]
    },
    "name": "Bösebrücke Einkauf"
}
```

### Obtain entity data by type

This example returns the data of all `Store` entities within the context data The `type` parameter limits the response
to store entities only.

#### 5 Request:

```bash
curl -G -X GET \
    'http://localhost:1026/v2/entities' \
    -d 'type=Store' \
    -d 'options=keyValues'
```

#### Response:

Because of the use of the `options=keyValues`, the response consists of JSON only without the attribute `type` and
`metadata` elements.

```json
[
    {
        "id": "urn:ngsi-ld:Store:001",
        "type": "Store",
        "address": {
            "streetAddress": "Bornholmer Straße 65",
            "addressRegion": "Berlin",
            "addressLocality": "Prenzlauer Berg",
            "postalCode": "10439"
        },
        "location": {
            "type": "Point",
            "coordinates": [13.3986, 52.5547]
        },
        "name": "Bose Brucke Einkauf"
    },
    {
        "id": "urn:ngsi-ld:Store:002",
        "type": "Store",
        "address": {
            "streetAddress": "Friedrichstraße 44",
            "addressRegion": "Berlin",
            "addressLocality": "Kreuzberg",
            "postalCode": "10969"
        },
        "location": {
            "type": "Point",
            "coordinates": [13.3903, 52.5075]
        },
        "name": "Checkpoint Markt"
    }
]
```

### Filter context data by comparing the values of an attribute

This example returns all stores with the `name` attribute _Checkpoint Markt_. Filtering can be done using the `q`
parameter - if a string has spaces in it, it can be URL encoded and held within single quote characters `'` = `%27`

#### 6 Request:

```bash
curl -G -X GET \
    'http://localhost:1026/v2/entities' \
    -d 'type=Store' \
    -d 'q=name==%27Checkpoint%20Markt%27' \
    -d 'options=keyValues'
```

#### Response:

Because of the use of the `options=keyValues`, the response consists of JSON only without the attribute `type` and
`metadata` elements.

```json
[
    {
        "id": "urn:ngsi-ld:Store:002",
        "type": "Store",
        "address": {
            "streetAddress": "Friedrichstraße 44",
            "addressRegion": "Berlin",
            "addressLocality": "Kreuzberg",
            "postalCode": "10969"
        },
        "location": {
            "type": "Point",
            "coordinates": [13.3903, 52.5075]
        },
        "name": "Checkpoint Markt"
    }
]
```

### Filter context data by comparing the values of a sub-attribute

This example returns all stores found in the Kreuzberg District.

Filtering can be done using the `q` parameter - sub-attributes are annotated using the dot syntax e.g.
`address.addressLocality`

#### 7 Request:

```bash
curl -G -X GET \
    'http://localhost:1026/v2/entities' \
    -d 'type=Store' \
    -d 'q=address.addressLocality==Kreuzberg' \
    -d 'options=keyValues'
```

#### Response:

Because of the use of the `options=keyValues`, the response consists of JSON only without the attribute `type` and
`metadata` elements.

```json
[
    {
        "id": "urn:ngsi-ld:Store:002",
        "type": "Store",
        "address": {
            "streetAddress": "Friedrichstraße 44",
            "addressRegion": "Berlin",
            "addressLocality": "Kreuzberg",
            "postalCode": "10969"
        },
        "location": {
            "type": "Point",
            "coordinates": [13.3903, 52.5075]
        },
        "name": "Checkpoint Markt"
    }
]
```

### Filter context data by querying metadata

This example returns the data of all `Store` entities with a verified address.

Metadata queries can be made using the `mq` parameter.

#### 8 Request:

```bash
curl -G -X GET \
    'http://localhost:1026/v2/entities' \
    -d 'type=Store' \
    -d 'mq=address.verified==true' \
    -d 'options=keyValues'
```

#### Response:

Because of the use of the `options=keyValues`, the response consists of JSON only without the attribute `type` and
`metadata` elements.

```json
[
    {
        "id": "urn:ngsi-ld:Store:001",
        "type": "Store",
        "address": {
            "streetAddress": "Bornholmer Straße 65",
            "addressRegion": "Berlin",
            "addressLocality": "Prenzlauer Berg",
            "postalCode": "10439"
        },
        "location": {
            "type": "Point",
            "coordinates": [13.3986, 52.5547]
        },
        "name": "Bösebrücke Einkauf"
    },
    {
        "id": "urn:ngsi-ld:Store:002",
        "type": "Store",
        "address": {
            "streetAddress": "Friedrichstraße 44",
            "addressRegion": "Berlin",
            "addressLocality": "Kreuzberg",
            "postalCode": "10969"
        },
        "location": {
            "type": "Point",
            "coordinates": [13.3903, 52.5075]
        },
        "name": "Checkpoint Markt"
    }
]
```

### Filter context data by comparing the values of a geo:json attribute

This example return all Stores within 1.5km the **Brandenburg Gate** in **Berlin** (_52.5162N 13.3777W_)

#### 9 Request:

```bash
curl -G -X GET \
  'http://localhost:1026/v2/entities' \
  -d 'type=Store' \
  -d 'georel=near;maxDistance:1500' \
  -d 'geometry=point' \
  -d 'coords=52.5162,13.3777'
```

#### Response:

Because of the use of the `options=keyValues`, the response consists of JSON only without the attribute `type` and
`metadata` elements.

```json
[
    {
        "id": "urn:ngsi-ld:Store:002",
        "type": "Store",
        "address": {
            "streetAddress": "Friedrichstraße 44",
            "addressRegion": "Berlin",
            "addressLocality": "Kreuzberg",
            "postalCode": "10969"
        },
        "location": {
            "type": "Point",
            "coordinates": [13.3903, 52.5075]
        },
        "name": "Checkpoint Markt"
    }
]
```

## Next Steps

Want to learn how to add more complexity to your application by adding advanced features? You can find out by reading
the other tutorials in this series:

### Iterative Development

The context of the store finder demo is very simple, it could easily be expanded to hold the whole of a stock management
system by passing in the current stock count of each store as context data to the
[ Scorpio Context Broker](https://fiware- Scorpio.readthedocs.io/en/latest/).

So far, so simple, but consider how this Smart application could be iterated:

-   Real-time dashboards could be created to monitor the state of the stock across each store using a visualization
    component. \[[Wirecloud](https://github.com/FIWARE/catalogue/blob/master/processing/README.md#Wirecloud)\]
-   The current layout of both the warehouse and store could be passed to the context broker so the location of the
    stock could be displayed on a map
    \[[Wirecloud](https://github.com/FIWARE/catalogue/blob/master/processing/README.md#Wirecloud)\]
-   User Management components \[[Wilma](https://github.com/FIWARE/catalogue/blob/master/security/README.md#Wilma),
    [AuthZForce](https://github.com/FIWARE/catalogue/blob/master/security/README.md#Authzforce),
    [Keyrock](https://github.com/FIWARE/catalogue/blob/master/security/README.md#Keyrock)\] could be added so that only
    store managers are able to change the price of items
-   A threshold alert could be raised in the warehouse as the goods are sold to ensure the shelves are not left empty
    [publish/subscribe function of [ Scorpio Context Broker](https://fiware- Scorpio.readthedocs.io/en/latest/)]
-   Each generated list of items to be loaded from the warehouse could be calculated to maximize the efficiency of
    replenishment
    \[[Complex Event Processing - CEP](https://github.com/FIWARE/catalogue/blob/master/processing/README.md#new-perseo-incubated)\]
-   A motion sensor could be added at the entrance to count the number of customers
    \[[IDAS](https://github.com/FIWARE/catalogue/blob/master/iot-agents/README.md)\]
-   The motion sensor could ring a bell whenever a customer enters
    \[[IDAS](https://github.com/FIWARE/catalogue/blob/master/iot-agents/README.md)\]
-   A series of video cameras could be added to introduce a video feed in each store
    \[[Kurento](https://github.com/FIWARE/catalogue/blob/master/processing/README.md#Kurento)\]
-   The video images could be processed to recognize where customers are standing within a store
    \[[Kurento](https://github.com/FIWARE/catalogue/blob/master/processing/README.md#Kurento)\]
-   By maintaining and processing historical data within the system, footfall and dwell time can be calculated -
    establishing which areas of the store attract the most interest \[connection through
    [Cygnus](https://github.com/FIWARE/catalogue/blob/master/core/README.md#Cygnus) to Apache Flink\]
-   Patterns recognizing unusual behaviour could be used to raise an alert to avoid theft
    \[[Kurento](https://github.com/FIWARE/catalogue/blob/master/processing/README.md#Kurento)\]
-   Data on the movement of crowds would be useful for scientific research - data about the state of the store could be
    published externally.
    \[[extensions to CKAN](https://github.com/FIWARE/catalogue/tree/master/data-publication#extensions-to-ckan)\]

Each iteration adds value to the solution through existing components with standard interfaces and therefore minimizes
development time.
