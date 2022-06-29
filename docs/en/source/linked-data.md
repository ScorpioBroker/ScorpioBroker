[![FIWARE Core Context Management](https://nexus.lab.fiware.org/repository/raw/public/badges/chapters/core.svg)](https://github.com/FIWARE/catalogue/blob/master/core/README.md)
[![NGSI LD](https://img.shields.io/badge/NGSI-LD-d6604d.svg)](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.01.01_60/gs_CIM009v010101p.pdf)
[![JSON LD](https://img.shields.io/badge/JSON--LD-1.1-f06f38.svg)](https://w3c.github.io/json-ld-syntax/)

**Description:** This tutorial introduces linked data concepts to the FIWARE Platform. The supermarket chain’s store
finder application is recreated using **NGSI-LD** and the differences between the **NGSI v2** and **NGSI-LD** interfaces
are highlighted and discussed. The tutorial is a direct analogue of the original getting started tutorial but uses API
calls from the **NGSI-LD** interface.

The tutorial uses [cUrl](https://ec.haxx.se/) commands throughout, but is also available as
[Postman documentation](https://fiware.github.io/tutorials.Linked-Data/)

[![Run in Postman](https://run.pstmn.io/button.svg)](https://github.com/ScorpioBroker/ScorpioBroker/blob/feature-80-temp/docs/en/source/Payloads/FIWARE%20Linked%20Data.postman_collection.json)

<hr class="core"/>

# Adding Linked Data concepts to FIWARE Data Entities.

> “Six degrees of separation doesn't mean that everyone is linked to everyone else in just six steps. It means that a
> very small number of people are linked to everyone else in a few steps, and the rest of us are linked to the world
> through those special few.”
>
> ― Malcolm Gladwell, The Tipping Point

The introduction to FIWARE [Getting Started tutorial](https://github.com/FIWARE/tutorials.Getting-Started) introduced
the [NGSI v2](https://fiware.github.io/specifications/OpenAPI/ngsiv2) interface that is commonly used to create and
manipulate context data entities. An evolution of that interface has created a supplementary specification called
[NGSI-LD](https://forge.etsi.org/swagger/ui/?url=https://forge.etsi.org/gitlab/NGSI-LD/NGSI-LD/raw/master/spec/updated/full_api.json)
as a mechanism to enhance context data entities through adding the concept of **linked data**. This tutorial will
introduce the background of the ideas behind the new interface and compare and contrast how to create and manipulate
data entities as linked data.

Additional tutorials in the series will further discuss data relationships an how to create context data entities using
linked data enabling the full knowledge graph to be traversed.

## What is Linked Data?

All users of the Internet will be familiar with the concept of hypertext links, the way that a link on one web page is
able to guide the browser to loading another page from a known location.

Whilst humans are able to understand relationship discoverability and how links work, computers find this much more
difficult, and require a well-defined protocol to be able to traverse from one data element to another held in a
separate location.

Creating a system of readable links for computers requires the use of a well defined data format
([JSON-LD](http://json-ld.org/)) and assignation of unique IDs
([URLs or URNs](https://stackoverflow.com/questions/4913343/what-is-the-difference-between-uri-url-and-urn)) for both
data entities and the relationships between entities so that semantic meaning can be programmatically retrieved from the
data itself.

Properly defined linked data can be used to help answer big data questions, and the data relationships can be traversed
to answer questions like _"Which products are currently available on the shelves of Store X and what prices are they
sold at?"_

### Video: What is Linked Data?

[![](https://fiware.github.io/tutorials.Step-by-Step/img/video-logo.png)](https://www.youtube.com/watch?v=4x_xzT5eF5Q "Introduction")

Click on the image above to watch an introductory video on linked data concepts

JSON-LD is an extension of JSON , it is a standard way of avoiding ambiguity when expressing linked data in JSON so that
the data is structured in a format which is parsable by machines. It is a method of ensuring that all data attributes
can be easily compared when coming from a multitude of separate data sources, which could have a different idea as to
what each attribute means. For example, when two data entities have a `name` attribute how can the computer be certain
that is refers to a _"Name of a thing"_ in the same sense (rather than a **Username** or a **Surname** or something).
URLs and data models are used to remove ambiguity by allowing attributes to have a both short form (such as `name`) and
a fully specified long form (such `http://schema.org/name`) which means it is easy to discover which attribute have a
common meaning within a data structure.

JSON-LD introduces the concept of the `@context` element which provides additional information allowing the computer to
interpret the rest of the data with more clarity and depth.

Furthermore the JSON-LD specification enables you to define a unique `@type` associating a well-defined
[data model](https://fiware-datamodels.readthedocs.io/en/latest/guidelines/index.html) to the data itself.

### Video: What is JSON-LD?

[![](https://fiware.github.io/tutorials.Step-by-Step/img/video-logo.png)](https://www.youtube.com/watch?v=vioCbTo3C-4 "JSON-LD")

Click on the image above to watch a video describing the basic concepts behind JSON-LD.

## What is NGSI-LD?

**NGSI-LD** is an evolution of the **NGSI v2** information model, which has been modified to improve support for linked
data (entity relationships), property graphs and semantics (exploiting the capabilities offered by JSON-LD). This work
has been conducted under the ETSI ISG CIM initiative and the updated specification has been branded as
[NGSI-LD](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.01.01_60/gs_CIM009v010101p.pdf). The main constructs
of NGSI-LD are: _Entity_, _Property_ and _Relationship_. NGSI-LD Entities (instances) can be the subject of Properties
or Relationships. In terms of the traditional NGSI v2 data model, Properties can be seen as the combination of an
attribute and its value. Relationships allow to establish associations between instances using linked data.

### NGSI v2 Data Model

As a reminder, the NGSI v2 data model is quite simple. It can be summarized as shown below:

![](https://fiware.github.io/tutorials.Linked-Data/img/ngsi-v2.png)

The core element of NGSI v2 is the data _entity_, typically a real object with a changing state (such as a **Store**, a
**Shelf** and so on). Entities have _attributes_ (such as `name` and `location`) and these in turn hold _metadata_ such
as `accuracy` - i.e. the accuracy of a `location` reading.

Every _entity_ must have a `type` which defines the sort of thing the entity describes, but giving an NGSI v2 entity the
`type=Store` is relatively meaningless as no-one is obliged to shape their own **Store** entities in the same fashion.
Similarly adding an attribute called `name` doesn't suddenly make it hold the same data as someone else's `name`
attribute.

Relationships can be defined using NGSI v2, but only so far as giving the attribute an appropriate attribute name
defined by convention ( e.g. starting with `ref`, such as `refManagedBy`) and assigning the attribute
`type=Relationship` which again is purely a naming convention with no real semantic weight.

### NGSI LD Data Model

The NGSI LD data model is more complex, with more rigid definitions of use which lead to a navigable knowledge graph.

![](https://fiware.github.io/tutorials.Linked-Data/img/ngsi-ld.png)

Once again, _entity_ can be considered to be the core element. Every entity must use a unique `id` which must be a URI,
often a [URN](https://en.wikipedia.org/wiki/Uniform_resource_name), there is also a `type`, used to define the structure
of the data held, which must also be a URI. This URI should correspond to a well-defined data model which can be found
on the web. For example the URI `https://uri.fiware.org/ns/data-models#Building` is used to define common data model for
a [Building](https://fiware-datamodels.readthedocs.io/en/latest/Building/Building/doc/spec/index.html).

_Entities_ can have _properties_ and _relationships_. Ideally the name of each _property_ should also be a well defined
URI which corresponds to a common concept found across the web (e.g. `http://schema.org/address` is a common URI for the
physical address of an item). The _property_ will also have a value which will reflect the state of that property (e.g
`name="Checkpoint Markt"`). Finally a property may itself have further properties (a.k.a. _properties-of-properties_)
which reflect further information about the property itself. Properties and relationships may in turn have a linked
embedded structure (of _properties-of-properties_ or _properties-of-relationships or relationships-of-properties_ or
_relationships-of-relationships_ etc.) which lead to the following:

An NGSI LD Data Entity (e.g. a supermarket):

-   Has an `id` which must be unique. For example `urn:ngsi-ld:Building:store001`,
-   Has `type` which should be a fully qualified URI of a well defined data model. For example
    `https://uri.fiware.org/ns/data-models#Building`. Authors can also use type names, as short hand strings for types,
    mapped to fully qualified URIs through the JSON-LD `@context`.
-   Has _property_ of the entity, for example, an `address` attribute which holds the address of the store. This can be
    expanded into `http://schema.org/address`, which is known as a fully qualified name
    ([FQN](https://en.wikipedia.org/wiki/Fully_qualified_name)).
-   The `address`, like any _property_ will have a _value_ corresponding to the _property_ `address` (e.g. _Bornholmer
    Straße 65, 10439 Prenzlauer Berg, Berlin_
-   Has a _property-of-a-property_ of the entity, for example a `verified` field for the `address`.
-   Has a _relationship_ of the entity, for example, a `managedBy` field where the relationship `managedBy` corresponds
    to another data entity : `urn:ngsi-ld:Person:bob-the-manager`
-   The relationship `managedBy`, may itself have a _property-of-a-relationship_ (e.g. `since`), this holds the date Bob
    started working the store
-   The relationship `managedBy`, may itself have a _relationship-of-a-relationship_ (e.g. `subordinateTo`), this holds
    the URN of the area manager above Bob in the hierarchy.

As you can see the knowledge graph is well defined and can be expanded indefinitely.

Relationships will be dealt with in more detail in a subsequent tutorial.

# Architecture
Our demo application will only make use of one FIWARE component - the
[Scorpio Broker](https://scorpio.readthedocs.io/en/latest/). Usage of the Scorpio Context Broker (with proper
context data flowing through it) is sufficient for an application to qualify as _“Powered by FIWARE”_.

![Deployment Architecture](figures/deploymentarchitecture.png)

The deployment architecture leverages the Spring Cloud framework that addresses lots of Micro-services concerns (e.g. scaling, monitoring, fault-tolerant, highly available, secure, decoupled, etc. ) and Kafka based distributed and scalable message queue infrastructure to provide high performance on message processing for a huge number of context requests which is usual in the IoT domain. 
The deployment architecture covers the high-level operations (Http based REST with method POST/GET/DELETE/PATCH) request flow from the external world to the Scorpio Broker system.  The external request is served through a unified service API gateway interface that exposes a single IP/port combination to be used for all services that the Scorpio Broker system can provide. 
In reality, each of the Scorpio Broker services will be implemented as a micro-service that can be deployed as an independent standalone unit in adistributed computing environment.

The necessary configuration information can be seen in the services section of the associated `docker-compose.yml` file:

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
- BUILD\_DIR\_RSUBMG = Registry/RegistrySubscriptionManager 
- BUILD\_DIR\_SUBMG =Core/SubscriptionManager
- JAR\_FILE\_BUILD\_ACS = AtContextServer-\${project.version}.jar 
- JAR\_FILE\_BUILD\_SCS = config-server-\${project.version}.jar 
- JAR\_FILE\_BUILD\_SES = eureka-server-\${project.version}.jar 
- JAR\_FILE\_BUILD\_SGW = gateway-\${project.version}.jar 
- JAR\_FILE\_BUILD\_HMG = HistoryManager-\${project.version}.jar 
- JAR\_FILE\_BUILD\_QMG = QueryManager-\${project.version}.jar 
- JAR\_FILE\_BUILD\_RMG = RegistryManager-\${project.version}.jar
- JAR\_FILE\_BUILD\_EMG = EntityManager-\${project.version}.jar 
- JAR\_FILE\_BUILD\_RSUBMG = RegistrySubscriptionManager-\${project.version}.jar 
- JAR\_FILE\_BUILD\_SUBMG = SubscriptionManager-\${project.version}.jar
- JAR\_FILE\_RUN\_ACS = AtContextServer.jar 
- JAR\_FILE\_RUN\_SCS =config-server.jar 
- JAR\_FILE\_RUN\_SES = eureka-server.jar 
- JAR\_FILE\_RUN\_SGW = gateway.jar 
- JAR\_FILE\_RUN\_HMG =HistoryManager.jar 
- JAR\_FILE\_RUN\_QMG = QueryManager.jar 
- JAR\_FILE\_RUN\_RMG = RegistryManager.jar 
- JAR\_FILE\_RUN\_EMG =EntityManager.jar 
- JAR\_FILE\_RUN\_RSUBMG = RegistrySubscriptionManager.jar 
- JAR\_FILE\_RUN\_SUBMG = SubscriptionManager.jar
```

# Creating a "Powered by FIWARE" app based on Linked Data

This tutorial recreates the same data entities as the initial _"Powered by FIWARE"_ supermarket finder app, but using
NGSI-LD linked data entities rather than NGSI v2.


## Creating Context Data

When creating linked data entities, it is important to use common data models. This will allow us to easily combine data
from multiple sources and remove ambiguity when comparing data coming from different sources.

Creating linked data using fully qualified names throughout would be painful, as each attribute would need to be a URI,
so JSON-LD introduces the idea of an `@context` attribute which can hold pointers to context definitions. To add a
FIWARE [Building](https://fiware-datamodels.readthedocs.io/en/latest/Building/Building/doc/spec/index.html) data entity,
the following `@context` would be required

```json
{
    "id": "urn:ngsi-ld:Building:store001",
    "type": "Building",
    ...  other data attributes
    "@context": [
        "https://fiware.github.io/data-models/context.jsonld",
        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld"
    ]
}
```

### Core Context

[https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld](https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld)
refers to the Core `@context` of NGSI-LD, this defines element such as `id` and `type` which are common to all NGSI
entities, as well as defining terms such as `Property` and `Relationship`. The core context is so fundamental to
NGSI-LD, that it is added by default to any `@context` sent to a request.

### FIWARE Data Models

[https://fiware.github.io/data-models/context.jsonld](https://fiware.github.io/data-models/context.jsonld) refers to the
definition of standard data models supplied by FIWARE. Adding this to the `@context` will load the definitions of all
the [data models](https://fiwaredata-models.readthedocs.io) defined by the FIWARE Foundation in collaboration with other
organizations such as [GSMA](https://www.gsma.com/) and [TM Forum](https://www.tmforum.org/). A summary of the FQNs
related to **Building** can be seen below:

```json
{
    "@context": {
        "Building": "https://uri.fiware.org/ns/data-models#Building",
        ... etc
        "address": "http://schema.org/address",
        "category": "https://uri.fiware.org/ns/data-models#category",
        "location": "http://uri.etsi.org/ngsi-ld/location",
        ...etc
    }
}
```

If we include this context definition, it means that we will be able to use short names for `Building`, `address`,
`location` for our entities, but computers will also be able to read the FQNs when comparing with other sources.

To create a valid **Building** data entity in the context broker, make a POST request to the
`http://localhost:9090/ngsi-ld/v1/entities` endpoint as shown below. It is essential that the appropriate
`Content-Type: application/ld+json` is also used, so that the data entity is recognized as Linked data.

#### 2 Request:

```bash
curl -iX POST \
  http://localhost:9090/ngsi-ld/v1/entities \
  -H 'Content-Type: application/ld+json' \
  -d '{
    "id": "urn:ngsi-ld:Building:store001",
    "type": "Building",
    "category": {
        "type": "Property",
        "value": ["commercial"]
    },
    "address": {
        "type": "Property",
        "value": {
            "streetAddress": "Bornholmer Straße 65",
            "addressRegion": "Berlin",
            "addressLocality": "Prenzlauer Berg",
            "postalCode": "10439"
        },
        "verified": {
            "type": "Property",
            "value": true
        }
    },
    "location": {
        "type": "GeoProperty",
        "value": {
             "type": "Point",
             "coordinates": [13.3986, 52.5547]
        }
    },
    "name": {
        "type": "Property",
        "value": "Bösebrücke Einkauf"
    },
    "@context": [
        "https://fiware.github.io/data-models/context.jsonld",
        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld"
    ]
}'
```

The first request will take some time, as the context broker must navigate and load all of the files mentioned in the
`@context`.

> **Note**: if `https://fiware.github.io/data-models/context.jsonld` is unavailable for some reason the request will
> fail
>
> For a working production system it is essential that the `@context` files are always available to ensure third parties
> can read the context. High availability infrastructure has not been considered for this tutorial to keep the
> architecture simple.

#### 3 Request:

Each subsequent entity must have a unique `id` for the given `type`

```bash
curl -iX POST \
  http://localhost:9090/ngsi-ld/v1/entities/ \
  -H 'Content-Type: application/ld+json' \
  -d '{
    "id": "urn:ngsi-ld:Building:store002",
    "type": "Building",
    "category": {
        "type": "Property",
        "value": ["commercial"]
    },
    "address": {
        "type": "Property",
        "value": {
            "streetAddress": "Friedrichstraße 44",
            "addressRegion": "Berlin",
            "addressLocality": "Kreuzberg",
            "postalCode": "10969"
        },
        "verified": {
            "type": "Property",
            "value": true
        }
    },
     "location": {
        "type": "GeoProperty",
        "value": {
             "type": "Point",
              "coordinates": [13.3903, 52.5075]
        }
    },
    "name": {
        "type": "Property",
        "value": "Checkpoint Markt"
    },
    "@context": [
        "https://fiware.github.io/data-models/context.jsonld",
        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld"
    ]
}'
```

### Defining Properties within the NGSI-LD entity definition

The attributes `id` and `type` should be familiar to anyone who has used NGSI v2, and these have not changed. As
mentioned above, the type should refer to an included data model, in this case `Building` is being used as a short name
for the included URN `https://uri.fiware.org/ns/data-models#Building`. Thereafter each _property_ is defined as a JSON
element containing two attributes, a `type` and a `value`.

The `type` of a _property_ attribute must be one of the following:

-   `"GeoProperty"`: `"http://uri.etsi.org/ngsi-ld/GeoProperty"` for locations. Locations should be specified as
    Longitude-Latitude pairs in [GeoJSON format](https://tools.ietf.org/html/rfc7946). The preferred name for the
    primary location attribute is `location`
-   `"Property"`: `"http://uri.etsi.org/ngsi-ld/Property"` - for everything else.
-   `"Property"` should also be used for all time-based values, but the property `value` should be Date, Time or
    DateTime strings encoded in the [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601) - e.g.
    `YYYY-MM-DDThh:mm:ssZ`

> **Note:** that for simplicity, this data entity has no relationships defined. Relationships must be given the
> `type="Relationship`. Relationships will be discussed in a subsequent tutorial.

### Defining Properties-of-Properties within the NGSI-LD entity definition

_Properties-of-Properties_ is the NGSI-LD equivalent of metadata (i.e. _"data about data"_), it is use to describe
properties of the attribute value itself like accuracy, provider, or the units to be used. Some built-in metadata
attributes already exist and these names are reserved:

-   `createdAt` (type: DateTime): attribute creation date as an ISO 8601 string.
-   `modifiedAt` (type: DateTime): attribute modification date as an ISO 8601 string.

Additionally `observedAt`, `datasetId` and `instanceId` may optionally be added in some cases, and `location`,
`observationSpace` and `operationSpace` have special meaning for Geoproperties.

In the examples given above, one element of metadata (i.e. a _property-of-a-property_) can be found within the `address`
attribute. a `verified` flag indicates whether the address has been confirmed. The commonest _property-of-a-property_ is
`unitCode` which should be used to hold the UN/CEFACT
[Common Codes](http://wiki.goodrelations-vocabulary.org/Documentation/UN/CEFACT_Common_Codes) for Units of Measurement.

## Querying Context Data

A consuming application can now request context data by making NGSI-LD HTTP requests to the  Scorpio Broker. The
existing NGSI-LD interface enables us to make complex queries and filter results and retrieve data with FQNs or with
short names.

### Obtain entity data by FQN Type

This example returns the data of all `Building` entities within the context data The `type` parameter is mandatory for
NGSI-LD and is used to filter the response. The Accept HTTP header is needed to retrieve JSON-LD content.

#### 4 Request:

```bash
curl -G -X GET \
  'http://localhost:9090/ngsi-ld/v1/entities' \
  -H 'Accept: application/ld+json' \
  -d 'type=https://uri.fiware.org/ns/data-models%23Building'
```

#### Response:

The response returns the Core `@context` by default (`https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld`) and
all attributes are expanded whenever possible.

-   `id`, `type`, `location` and `name` are defined in the core context and are not expanded.
-   `address` has been mapped to `http://schema.org/address`
-   `category` has been mapped to `https://uri.fiware.org/ns/data-models#category`

Note that if an attribute has not been not associated to an FQN when the entity was created, the short name will
**always** be displayed.

```json
[
    {
        "id": "urn:ngsi-ld:Building:store001",
        "type": "https://uri.fiware.org/ns/data-models#Building",
        "http://schema.org/address": {
            "type": "Property",
            "value": {
                "streetAddress": "Bornholmer Straße 65",
                "addressRegion": "Berlin",
                "addressLocality": "Prenzlauer Berg",
                "postalCode": "10439"
            },
            "verified": {
                "type": "Property",
                "value": true
            }
        },
        "name": {
            "type": "Property",
            "value": "Bösebrücke Einkauf"
        },
        "https://uri.fiware.org/ns/data-models#category": {
            "type": "Property",
            "value": ["commercial"]
        },
        "location": {
            "type": "GeoProperty",
            "value": {
                "type": "Point",
                "coordinates": [13.3986, 52.5547]
            }
        },
        "@context": "https://uri.etsi.org/ngsi-lv1/ngsi-ld-core-context-v1.3.jsonld"
    },
    {
        "id": "urn:ngsi-ld:Building:store002",
        "type": "https://uri.fiware.org/ns/data-models#Building",
        "http://schema.org/address": {
            "type": "Property",
            "value": {
                "streetAddress": "Friedrichstraße 44",
                "addressRegion": "Berlin",
                "addressLocality": "Kreuzberg",
                "postalCode": "10969"
            },
            "verified": {
                "type": "Property",
                "value": true
            }
        },
        "name": {
            "type": "Property",
            "value": "Checkpoint Markt"
        },
        "https://uri.fiware.org/ns/data-models#category": {
            "type": "Property",
            "value": ["commercial"]
        },
        "location": {
            "type": "GeoProperty",
            "value": {
                "type": "Point",
                "coordinates": [13.3903, 52.5075]
            }
        },
        "@context": "https://uri.etsi.org/ngsi-lv1/ngsi-ld-core-context-v1.3.jsonld"
    }
]
```

### Obtain entity data by ID

This example returns the data of `urn:ngsi-ld:Building:store001`

#### 5 Request:

```bash
curl -G -X GET \
  -H 'Accept: application/ld+json' \
   'http://localhost:9090/ngsi-ld/v1/entities/urn:ngsi-ld:Building:store001'
```

#### Response:

The response returns the Core `@context` by default (`https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld`) and
all attributes are expanded whenever possible.

```json
{
    "id": "urn:ngsi-ld:Building:store001",
    "type": "https://uri.fiware.org/ns/data-models#Building",
    "http://schema.org/address": {
        "type": "Property",
        "value": {
            "streetAddress": "Bornholmer Straße 65",
            "addressRegion": "Berlin",
            "addressLocality": "Prenzlauer Berg",
            "postalCode": "10439"
        },
        "verified": {
            "type": "Property",
            "value": true
        }
    },
    "name": {
        "type": "Property",
        "value": "Bösebrücke Einkauf"
    },
    "https://uri.fiware.org/ns/data-models#category": {
        "type": "Property",
        "value": ["commercial"]
    },
    "location": {
        "type": "GeoProperty",
        "value": {
            "type": "Point",
            "coordinates": [13.3986, 52.5547]
        }
    },
    "@context": "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld"
}
```

### Obtain entity data by type

If a reference to the supplied data is supplied, it is possible to return short name data and limit responses to a
specific `type` of data. For example, the request below returns the data of all `Building` entities within the context
data. Use of the `type` parameter limits the response to `Building` entities only, use of the `options=keyValues` query
parameter reduces the response down to standard JSON-LD.

A [`Link` header](https://www.w3.org/wiki/LinkHeader) must be supplied to associate the short form `type="Building"`
with the FQN `https://uri.fiware.org/ns/data-models#Building`. The full link header syntax can be seen below:

```text
Link: <https://fiware.github.io/data-models/context.jsonld>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json
```

The standard HTTP `Link` header allows metadata (in this case the `@context`) to be passed in without actually touching
the resource in question. In the case of NGSI-LD, the metadata is a file in `application/ld+json` format.

#### 6 Request:

```bash
curl -G -X GET \
  'http://localhost:9090/ngsi-ld/v1/entities' \
    -H 'Link: <https://fiware.github.io/data-models/context.jsonld>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"' \
    'http://localhost:9090/ngsi-ld/v1/entities' \
    -H 'Accept: application/ld+json' \
    -d 'type=Building' \
    -d 'options=keyValues'
```

#### Response:

Because of the use of the `options=keyValues`, the response consists of JSON only without the attribute definitions
`type="Property"` or any _properties-of-properties_ elements. You can see that `Link` header from the request has been
used as the `@context` returned in the response.

```json
[
    {
        "id": "urn:ngsi-ld:Building:store001",
        "type": "Building",
        "address": {
            "streetAddress": "Bornholmer Straße 65",
            "addressRegion": "Berlin",
            "addressLocality": "Prenzlauer Berg",
            "postalCode": "10439"
        },
        "name": "Bösebrücke Einkauf",
        "category": ["commercial"],
        "location": {
            "type": "Point",
            "coordinates": [13.3986, 52.5547]
        },
        "@context": "https://fiware.github.io/data-models/context.jsonld"
    },
    {
        "id": "urn:ngsi-ld:Building:store002",
        "type": "Building",
        "address": {
            "streetAddress": "Friedrichstraße 44",
            "addressRegion": "Berlin",
            "addressLocality": "Kreuzberg",
            "postalCode": "10969"
        },
        "name": "Checkpoint Markt",
        "category": ["commercial"],
        "location": {
            "type": "Point",
            "coordinates": [13.3903, 52.5075]
        },
        "@context": "https://fiware.github.io/data-models/context.jsonld"
    }
]
```

### Filter context data by comparing the values of an attribute

This example returns all `Building` entities with the `name` attribute _Checkpoint Markt_. Filtering can be done using
the `q` parameter - if a string has spaces in it, it can be URL encoded and held within double quote characters `"` =
`%22`.

#### 7 Request:

```bash
curl -G -X GET \
    'http://localhost:9090/ngsi-ld/v1/entities' \
    -H 'Link: <https://fiware.github.io/data-models/context.jsonld>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'
    -H 'Accept: application/ld+json' \
    -d 'type=Building' \
    -d 'q=name==%22Checkpoint%20Markt%22' \
    -d 'options=keyValues'
```

#### Response:

The `Link` header `https://fiware.github.io/data-models/context.jsonld` holds an array of `@context` as shown:

```json
{
    "@context": [
        "https://fiware.github.io/data-models/context.jsonld",
        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld"
    ]
}
```

and therefore includes the FIWARE Building model.

This means that use of the `Link` header and the `options=keyValues` parameter reduces the response to short form
JSON-LD as shown:

```json
[
    {
        "id": "urn:ngsi-ld:Building:store002",
        "type": "Building",
        "address": {
            "streetAddress": "Friedrichstraße 44",
            "addressRegion": "Berlin",
            "addressLocality": "Kreuzberg",
            "postalCode": "10969"
        },
        "name": "Checkpoint Markt",
        "category": ["commercial"],
        "location": {
            "type": "Point",
            "coordinates": [13.3903, 52.5075]
        },
        "@context": "https://fiware.github.io/data-models/context.jsonld"
    }
]
```

### Filter context data by comparing the values of an attribute in an Array

Within the standard `Building` model, the `category` attribute refers to an array of strings. This example returns all
`Building` entities with a `category` attribute which contains either `commercial` or `office` strings. Filtering can be
done using the `q` parameter, comma separating the acceptable values.

#### 8 Request:

```bash
curl -G -X GET \
    'http://localhost:9090/ngsi-ld/v1/entities' \
    -H 'Link: <https://fiware.github.io/data-models/context.jsonld>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'
    -H 'Accept: application/ld+json' \
    -d 'type=Building' \
    -d 'q=category==%22commercial%22,%22office%22 \
    -d 'options=keyValues'
```

#### Response:

The response is returned in JSON-LD format with short form attribute names:

```json
[
    {
        "id": "urn:ngsi-ld:Building:store001",
        "type": "Building",
        "address": {
            "streetAddress": "Bornholmer Straße 65",
            "addressRegion": "Berlin",
            "addressLocality": "Prenzlauer Berg",
            "postalCode": "10439"
        },
        "name": "Bösebrücke Einkauf",
        "category": ["commercial"],
        "location": {
            "type": "Point",
            "coordinates": [13.3986, 52.5547]
        },
        "@context": "https://fiware.github.io/data-models/context.jsonld"
    },
    {
        "id": "urn:ngsi-ld:Building:store002",
        "type": "Building",
        "address": {
            "streetAddress": "Friedrichstraße 44",
            "addressRegion": "Berlin",
            "addressLocality": "Kreuzberg",
            "postalCode": "10969"
        },
        "name": "Checkpoint Markt",
        "category": ["commercial"],
        "location": {
            "type": "Point",
            "coordinates": [13.3903, 52.5075]
        },
        "@context": "https://fiware.github.io/data-models/context.jsonld"
    }
]
```

### Filter context data by comparing the values of a sub-attribute

This example returns all stores found in the Kreuzberg District.

Filtering can be done using the `q` parameter - sub-attributes are annotated using the bracket syntax e.g.
`q=address[addressLocality]=="Kreuzberg"`. This differs from NGSI v2 where dot syntax was used.

#### 9 Request:

```bash
curl -G -X GET \
    'http://localhost:9090/ngsi-ld/v1/entities' \
    -H 'Link: <https://fiware.github.io/data-models/context.jsonld>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'
    -H 'Accept: application/ld+json' \
    -d 'type=Building' \
    -d 'q=address[addressLocality]==%22Kreuzberg%22' \
    -d 'options=keyValues'
```

#### Response:

Use of the `Link` header and the `options=keyValues` parameter reduces the response to JSON-LD.

```json
[
    {
        "id": "urn:ngsi-ld:Building:store002",
        "type": "Building",
        "address": {
            "streetAddress": "Friedrichstraße 44",
            "addressRegion": "Berlin",
            "addressLocality": "Kreuzberg",
            "postalCode": "10969"
        },
        "name": "Checkpoint Markt",
        "category": ["commercial"],
        "location": {
            "type": "Point",
            "coordinates": [13.3903, 52.5075]
        },
        "@context": "https://fiware.github.io/data-models/context.jsonld"
    }
]
```

### Filter context data by querying metadata

This example returns the data of all `Building` entities with a verified address. The `verified` attribute is an example
of a _Property-of-a-Property_

Metadata queries (i.e. Properties of Properties) are annotated using the dot syntax e.g. `q=address.verified==true`.
This supersedes the `mq` parameter from NGSI v2.

#### 10 Request:

```bash
curl -G -X GET \
    'http://localhost:9090/ngsi-ld/v1/entities' \
    -H 'Link: <https://fiware.github.io/data-models/context.jsonld>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'
    -H 'Accept: application/json' \
    -d 'type=Building' \
    -d 'mq=address.verified==true' \
    -d 'options=keyValues'
```

#### Response:

Because of the use of the `options=keyValues`, together with the Accept HTTP header (`application/json`), the response
consists of **JSON only** without an `@context` or attribute `type` and `metadata` elements.

```json
[
    {
        "id": "urn:ngsi-ld:Building:store001",
        "type": "Building",
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
        "id": "urn:ngsi-ld:Building:store002",
        "type": "Building",
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

This example return all Stores within 2km the **Brandenburg Gate** in **Berlin** (_52.5162N 13.3777W_). To make a
geo-query request, three parameters must be specified, `geometry`, `coordinates` and `georel`.

The syntax for NGSI-LD has been updated, the `coordinates` parameter is now represented in
[geoJSON](https://tools.ietf.org/html/rfc7946) including the square brackets rather than the simple lat-long pairs
required in NGSI v2.

Note that by default the geo-query will be applied to the `location` attribute, as this is default specified in NGSI-LD.
If another attribute is to be used, an additional `geoproperty` parameter is required.

#### 11 Request:

```bash
curl -G -X GET \
  'http://localhost:9090/ngsi-ld/v1/entities' \
  -H 'Link: <https://fiware.github.io/data-models/context.jsonld>; rel="http://www.w3.org/ns/json-ld#context"; type="application/json"'
  -H 'Accept: application/json' \
  -d 'type=Building' \
  -d 'geometry=Point' \
  -d 'coordinates=[13.3777,52.5162]' \
  -d 'georel=near;maxDistance==2000' \
  -d 'options=keyValues'
```

#### Response:

Because of the use of the `options=keyValues` together with the Accept HTTP header (`application/json`), the response
consists of **JSON only** without an `@context` or attribute `type` and `metadata` elements.

```json
[
    {
        "id": "urn:ngsi-ld:Building:store002",
        "type": "Building",
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
