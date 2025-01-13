# <img src="./img/ScorpioLogo.svg" width="140" align="middle"> Scorpio NGSI-LD Broker

[![FIWARE Core](https://nexus.lab.fiware.org/static/badges/chapters/core.svg)](https://www.fiware.org/developers/catalogue/)
[![License: BSD-3-Clause](https://img.shields.io/badge/license-BSD%203%20Clause-blue.svg)](https://spdx.org/licenses/BSD-3-Clause.html)
[![fiware](https://nexus.lab.fiware.org/repository/raw/public/badges/stackoverflow/fiware.svg)](https://stackoverflow.com/questions/tagged/fiware)
![NGSI-LD badge](https://img.shields.io/badge/NGSI-LD-red.svg) 
<br/>
[![Quay badge](https://img.shields.io/badge/quay.io-fiware%2Fscorpio-grey?logo=red%20hat&labelColor=EE0000)](https://quay.io/repository/fiware/scorpio)
[![Docker badge](https://img.shields.io/badge/docker-scorpiobroker%2Fall--in--one--runner-blue?logo=docker)](https://hub.docker.com/r/scorpiobroker/all-in-one-runner)
<br/>
[![Documentation badge](https://img.shields.io/readthedocs/scorpio.svg)](https://scorpio.readthedocs.io/en/latest/?badge=latest) 
![Status](https://nexus.lab.fiware.org/static/badges/statuses/full.svg) 
![NGSI-LD Test Suite Java](https://github.com/scorpiobroker/scorpiobroker/actions/workflows/api-test.yml/badge.svg) 
![NGSI-LD Test Suite Native Linux](https://github.com/scorpiobroker/scorpiobroker/actions/workflows/api-test-linux-amd64.yml/badge.svg) 
![PMD](https://github.com/scorpiobroker/scorpiobroker/actions/workflows/pmd.yml/badge.svg) 
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/4781/badge)](https://bestpractices.coreinfrastructure.org/projects/4781)

Hello there!

Welcome to the Scorpio NGSI-LD Broker.
You are currently in the Scorpios main branch.

To get you quickly started you can just choose on of the provided docker-compose files and start it up. 
 - [java based all-in-one runner](https://raw.githubusercontent.com/ScorpioBroker/ScorpioBroker/development-quarkus/compose-files/docker-compose-java-aaio-kafka.yml)
 - [java based all-in-one runner without kafka integration, no scaling but less resource requirements](https://raw.githubusercontent.com/ScorpioBroker/ScorpioBroker/development-quarkus/compose-files/docker-compose-java-aaio.yml)
 - [java based distributed microservices](https://raw.githubusercontent.com/ScorpioBroker/ScorpioBroker/development-quarkus/compose-files/docker-compose-java-dist-kafka.yml)
 - [Linux AMD 64 native all-in-one runner](https://raw.githubusercontent.com/ScorpioBroker/ScorpioBroker/development-quarkus/compose-files/docker-compose-linux-amd64-aaio-kafka.yml)
 - [Linux AMD 64 native all-in-one runner without kafka integration, no scaling but less resource requirements](https://raw.githubusercontent.com/ScorpioBroker/ScorpioBroker/development-quarkus/compose-files/docker-compose-linux-amd64-aaio.yml)
 - [Linux AMD 64 native distributed microservices](https://raw.githubusercontent.com/ScorpioBroker/ScorpioBroker/development-quarkus/compose-files/docker-compose-linux-amd64-dist-kafka.yml)

We also provide all docker images on the [Amazon Public ECR](https://gallery.ecr.aws/scorpiobroker?page=1)

<h2>Which option to choose?</h2>

1. java vs. native

We have a regular java version, which should run on any machine running java, and we have native compilations, currently primarily for linux amd64. There is a trade-off between the java version and the native one - the java one is faster, but the native one uses less memory - i.e. if your environment is memory constraint, you should go for a native version, otherwise, we would recommend the java version

2. all-in-one vs. micro-services

The implementation architecture of Scorpio is based on micro services, which allows individual scaling with micro services (e.g. assuming you need a lot of subscriptions, you create multiple instances of the subscription service) in the cloud. If there are multiple micro services, they need to communicate with each other and this is done using a message bus - the standard message bus is kafka, but also mqtt and rabbitmq are supported - a special version is sqs, which is the native message bus of AWS, which they use when Scorpio is used in their cloud as part of the Garnet framework - if you are not using AWS, do not use sqs. If you do not need to scale in the cloud, you can use the all-in-one-runner (aaio), which can also work with a message bus, but it does not have to, i.e. java-aaio uses direct internal communication (i guess a kind of "pseudo" message bus).

3. distroless docker image vs linux base image

When using a docker, a base version for the docker container image is needed. We use some kind of minimum linux based image - which changed occasionally (also due to security holes that are discovered). Since the last version, we also support a "distroless image" - which is a bare minimum docker image..

4. Local DB and Messagebus setup
   
If you have a local PostgreSQL (and Kafka) setup you can also download pre builds for Ubuntu, Windows and MacOS from the [releases](https://github.com/ScorpioBroker/ScorpioBroker/releases)

<h2>Where to get examples</h2>

To get a good collection of NGSI-LD operations and examples we recommend to have a look at our [test suite](https://raw.githubusercontent.com/ScorpioBroker/ScorpioBroker/development-quarkus/api-test.json). 
It is Postman based and you can just import it together with the respective environment settings [all-in-one](https://raw.githubusercontent.com/ScorpioBroker/ScorpioBroker/development-quarkus/api-test-aaio-environment.json) or [distributed](https://raw.githubusercontent.com/ScorpioBroker/ScorpioBroker/development-quarkus/api-test-dist-environment.json)

<h2>What is Scorpio and NGSI-LD</h2>

Scorpio is an NGSI-LD compliant context broker developed by NEC Laboratories Europe and NEC Technologies India. It implements the full [NGSI-LD API](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.02.02_60/gs_CIM009v010202p.pdf) as specified by the ETSI Industry Specification Group on cross cutting Context Information Management ([ETSI ISG CIM](https://www.etsi.org/committee/cim)).

The NGSI-LD API enables the management, access and discovery of context information. Context information consists of *entities* (e.g. a building) and their *properties* (e.g. address and geographic location) and *relationships* (e.g. owner). Thus Scorpio enables applications and services to request context information – what they need, when they need it and how they need it.

The functionalities of the NGSI-LD API include:
- Create, update, append and delete context infomration.
- Query context information, including filtering, geographic scoping and paging.
- Subscribe to changes in context information and receive asynchronous notifications.
- Register and discover sources of context information, which allows building distributed and federated deployments.

Scorpio is a FIWARE Generic Enabler. Therefore, it can be integrated as part of any platform “Powered by FIWARE”. FIWARE is a curated framework of open source platform components which can be assembled together with other third-party platform components to accelerate the development of Smart Solutions. The roadmap of this FIWARE GE is described [here](./docs/roadmap.md).

You can find more info at the [FIWARE developers](https://developers.fiware.org/) website and the [FIWARE](https://fiware.org/) website.
The complete list of FIWARE GEs and Incubated FIWARE GEs can be found in the [FIWARE Catalogue](https://catalogue.fiware.org/).

| :books: [Documentation](https://scorpio.rtfd.io/) | :mortar_board: [Academy](https://fiware-academy.readthedocs.io/en/latest/core/scorpio) | <img style="height:1em" src="https://quay.io/static/img/quay_favicon.png"/> [quay.io](https://quay.io/repository/fiware/scorpio) | :whale: [Docker Hub](https://hub.docker.com/r/scorpiobroker) | :dart: [Roadmap](./docs/roadmap.md) |
| ------------------------------------------------- | --------------------------------------------------------------------- | --------------------------------------------------------------------- | --------------------------------------------------------------------- | --- |

## Content

-   [Background](#background)
-   [Installation](#installation-and-building)
-   [Usage](#usage)
-   [API Walkthrough](#api-walkthrough)
-   [Tests](#tests)
-   [Further Resources](#further-resources)
-   [Credit where credit is due](#credit-where-credit-is-due)
-   [Code of conduct](#code-of-conduct)
-   [License](#license)

## Background

Scorpio is an NGSI-LD Broker that allows managing and requesting context information. It supports the following functionalities:
- Context Producers can manage their context – creating, updating, appending and deleting context information. 
- Context Consumers can request the context information they require, either identifying the entity or discover relevant entities by providing the entity type, possibly filtering according to property value, existing relationship and/or geographic scope provided as a GeoJSON feature. 
- Two interaction styles are supported, a synchronous query-response, and an asynchronous subscribe / notify, where notifications can be based on a change in property or relationship, or on a fixed time interval. 
-  Scorpio implements NGSI-LD’s optional temporal interface for requesting historic information, e.g. the property values measured within a specified time interval.
- Scorpio supports multiple deployment configurations including centralized, distributed and federated ones. In addition to the Context Producers mentioned above, there can be Context Sources that themselves implement the NGSI-LD interface. These Context Sources can register themselves with the information they can provide on request (not the information (values) itself). A Scorpio Broker in a distributed setting can then discover the Context Sources that may have information for answering a request based on their registrations, request and aggregate the information from different Context Sources and provide it to the requesting Context Consumer. 
- In a federated setting the Context Source can itself be an NGSI-LD Broker. Federations can be used to combine information from multiple providers that want to (partially) share information. An important difference is then typically in the granularity of the registration, e.g. “I have information about entities of entity type building within a geographic area”, instead of “I have information about Building A”
- Scorpio supports all the mentioned deployment configurations. Thus it provides scalability and the possibility of extending scenarios in an evolutionary way. For example two separate deployments can be combined or for scalability reasons different brokers can be used – completely transparent to Context Consumers that can still use a single point of access.

## Installation and Building

Scorpio is developed in Java using Quarkus as microservice framework and Apache Maven as build tool. It requires Apache Kafka as a message bus and Postgres with PostGIS extensions as database.

The easiest way to get Scorpio running is using docker-compose. Please have a look [here](./compose-files) for examples.

## Configuration

Scorpio can be configured via environment variables. Please have a look [here](./AllInOneRunner/src/main/resources/application.properties) for a full list of config options. 
To configure via environment variable provide the config option in all capital letters and replace the dot with an underscore. So e.g. quarkus.http.port becomes QUARKUS_HTTP_PORT.

The most common parameters are the following. 
#The port for the HTTP API
quarkus.http.port=9090
#The PostgreSQL related settings
dbhost=localhost 
dbport=5432
dbuser=ngb
dbpass=ngb
dbname=ngb (This is the name of the default database. Tenant databases will be prefixed with this setting.)
#Kafka related settings 
bushost=localhost
busport=9092
#Settings used by the auto registry and federation feature. This should be the public reachable name or IP of the Scorpio installation.
gateway.host=localhost
gateway.port=9090


## Usage

By default the broker runs on port 9090 the base URL for interaction with the broker would be than
http://localhost:9090/ngsi-ld/v1/ 

### Simple Example

Generally speaking you can Create entities by sending an HTTP POST request to http://localhost:9090/ngsi-ld/v1/entities/
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

## API Walkthrough

More detailed examples of what you can do with the NGSI-LD API provided by Scorpio can be found in the [API Walkthrough](./docs/en/source/API_walkthrough.rst).

## Tests
Scorpio has two sets of tests. We use JUnit for unit tests and the FIWARE NGSI-LD Testsuite, which is npm test based, for system tests.
Further details about testing can be found in the [Testing Guide](./docs/en/source/testing.rst).

## Further resources

For more detailed explaination on NGSI-LD or JSON-LD. Please look at the
-  [ETSI Specification](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.02.02_60/gs_CIM009v010202p.pdf) 
-  [ETSI Primer](https://www.etsi.org/deliver/etsi_gr/CIM/001_099/008/01.01.01_60/gr_CIM008v010101p.pdf)
-  [JSON-LD website](https://json-ld.org/)
-  [Scorpio in FIWARE Academy](https://fiware-academy.readthedocs.io/en/latest/core/scorpio/index.html)
  -  [FIWARE 601: Introduction to Linked Data](https://fiware-tutorials.readthedocs.io/en/latest/linked-data)
  -  [FIWARE 602: Linked Data Relationships and Data Models](https://fiware-tutorials.readthedocs.io/en/latest/relationships-linked-data)

-  [FIWARE global summit: The Scorpio NGSI-LD Broker. Features and supported architectures](https://www.slideshare.net/FI-WARE/fiware-global-summit-the-scorpio-ngsild-broker-features-and-supported-architectures)
-  [FIWARE global summit: NGSI-LD. An evolution from NGSI V2](https://www.slideshare.net/FI-WARE/fiware-global-summit-ngsild-an-evolution-from-ngsiv2)


You can find a set of example calls, as a Postman collection, in the Examples folder. These examples use 2 Variables

- gatewayServer, which has to be `<brokerIP>:<brokerPort>`. When using default settings locally it would be localhost:9090
- link, which is for the examples providing @context via the Link header. For the examples we host an example @context. Set link to https://raw.githubusercontent.com/ScorpioBroker/ScorpioBroker/master/Examples/index.json

## Acknowledgements

### EU Acknowledgetment
This activity has received funding from the European Union’s Horizon 2020 research and innovation programme under Grant Agreement No. 2020-EU-IA-0274 (SALTED), No. INEA/CEF/ICT/A2019/2063604 (ODALA), No. 731993 (Autopilot), No. 814918 (Fed4IoT) and No. 767498 (MIDIH, Open Call (MoLe). 
<img src="https://raw.githubusercontent.com/ScorpioBroker/ScorpioBroker/master/img/flag_yellow_low.jpg" width="160">
- [ODALA project](https://odalaproject.eu/)
- [SALTED project](https://salted-project.eu/)
- [AUTOPILOT project: Automated driving Progressed by Internet Of Things](https://autopilot-project.eu/) <img src="https://raw.githubusercontent.com/ScorpioBroker/ScorpioBroker/master/img/autopilot.png" width="160">
- [Fed4IoT project](https://fed4iot.org/)
- [MIDIH Project](https://midih.eu/), Open Call (MoLe)

## Credit where credit is due
We like to thank everyone who has contributed to Scorpio. This goes for the entire Scorpio Devlopment Team as well as all external contributor.
For a complete list have a look at the [CREDITS](./CREDITS) file.

## Code of conduct
As part of the FIWARE Community we try our best to adhere to the [FIWARE Code of Conduct](https://www.fiware.org/foundation/code-of-conduct/) and expect the same from contributors. 

This includes pull requests, issues, comments, code and in code comments. 

As owner of this repo we limit communication here purely to Scorpio and NGSI-LD related topics. 

We are all humans coming from different cultural backgrounds. We all have our different quirks, habits and mannerisms. Therefor misunderstandings can happen. We will give everyone the benefit of doubt that communication is done with good intentions in mind trying to advance Scorpio and NGSI-LD. We expect the same from contributors.
However if someone is repeatedly trying to provoke, attack a person, shift discussions or ridicule someone we WILL make use of our house right and put an end to this.

If there is a dispute to be resolved we as owners of this repo have the final word.
## License

Scorpio is licensed under [BSD-3-Clause](https://spdx.org/licenses/BSD-3-Clause.html).
For contributions this [contribution license](CONTRIBUTING.md) applies.

© 2021 NEC
