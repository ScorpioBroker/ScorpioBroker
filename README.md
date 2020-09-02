# <img src="./img/ScorpioLogo.svg" width="140" align="middle"> Scorpio NGSI-LD Broker

[![FIWARE Core](https://nexus.lab.fiware.org/static/badges/chapters/core.svg)](https://www.fiware.org/developers/catalogue/)
[![License: BSD-4-Clause](https://img.shields.io/badge/license-BSD%204%20Clause-blue.svg)](https://spdx.org/licenses/BSD-4-Clause.html)
[![Docker](https://img.shields.io/docker/pulls/scorpiobroker/scorpio.svg)](https://hub.docker.com/r/scorpiobroker/scorpio/)
[![fiware](https://nexus.lab.fiware.org/repository/raw/public/badges/stackoverflow/fiware.svg)](https://stackoverflow.com/questions/tagged/fiware)
[![NGSI LD](https://nexus.lab.fiware.org/repository/raw/public/badges/specifications/ngsild.svg)](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.02.02_60/gs_CIM009v010202p.pdf)
<br>
[![Documentation badge](https://img.shields.io/readthedocs/scorpio.svg)](https://scorpio.readthedocs.io/en/latest/?badge=latest)
![Status](https://nexus.lab.fiware.org/static/badges/statuses/incubating.svg)
![Travis-CI](https://travis-ci.org/ScorpioBroker/ScorpioBroker.svg?branch=master)

Scorpio is an NGSI-LD compliant context broker developed by NEC Laboratories Europe and NEC Technologies India. It implements the full [NGSI-LD API](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.02.02_60/gs_CIM009v010202p.pdf) as specified by the ETSI Industry Specification Group on cross cutting Context Information Management ([ETSI ISG CIM](https://www.etsi.org/committee/cim)).

The NGSI-LD API enables the management, access and discovery of context information. Context information consists of *entities* (e.g. a building) and their *properties* (e.g. address and geographic location) and *relationships* (e.g. owner).

The functionalities of the NGSI-LD API include:
- Create, update, append and delete context infomration.
- Query context information, including filtering, geographic scoping and paging.
- Subscribe to changes in context information and receive asynchronous notifications.
- Register and discover sources of context information, which allows building distributed and federated deployments.

Scorpio is a FIWARE Generic Enabler. Therefore, it can be integrated as part of any platform “Powered by FIWARE”. FIWARE is a curated framework of open source platform components which can be assembled together with other third-party platform components to accelerate the development of Smart Solutions.

You can find more info at the [FIWARE developers](https://developers.fiware.org/) website and the [FIWARE](https://fiware.org/) website.
The complete list of FIWARE GEs and Incubated FIWARE GEs can be found in the [FIWARE Catalogue](https://catalogue.fiware.org/).

| :books: [Documentation](https://scorpio.rtfd.io/) | :mortar_board: [Academy](https://fiware-academy.readthedocs.io/en/latest/core/scorpio) | :whale: [Docker Hub](https://hub.docker.com/r/scorpiobroker/scorpio/) | :dart: [Roadmap](./docs/roadmap.md) |
| ------------------------------------------------- | --------------------------------------------------------------------- | --------------------------------------------------------------------- | --------------------------------------------------------------------- |

## Content

-   [Background](#background)
-   [Installation](#installation)
-   [Usage](#usage)
-   [API Walkthrough](#api-walkthrough)
-   [Tests](#tests)
-   [Further Resources](#further-resources)
-   [Credit where credit is due](#credit-where-credit-is-due)
-   [Code of conduct](#code-of-conduct)
-   [License](#license)

## Background

NGSI-LD is an open API and data model specification for context management
[published by ETSI](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.02.02_60/gs_CIM009v010202p.pdf).

## Installation and Building

Scorpio is developed in Java using SpringCloud as microservice framework and Apache Maven as build tool. It requires Apache Kafka as a message bus and Postgres with PostGIS extensions as database.

Information on how to install the software components required by Scorpio can be found in the [Installation Guide](https://github.com/ScorpioBroker/ScorpioBroker/blob/prepare_ge/docs/en/source/installationGuide.rst). For building and running Scorpio, you find instructions in the [Building and Running Scorpio Guide](https://github.com/ScorpioBroker/ScorpioBroker/blob/prepare_ge/docs/en/source/buildScorpio.rst).

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

More detailed examples of what you can do with the NGSI-LD API provided by Scorpio can be found in the [API Walkthrough](https://github.com/ScorpioBroker/ScorpioBroker/blob/prepare_ge/docs/en/source/API_walkthrough.rst).

## Tests
Scorpio has two sets of tests. We use JUnit for unit tests and the FIWARE NGSI-LD Testsuite, which is npm test based, for system tests.
Further details about testing can be found in the [Testing Guide](https://github.com/ScorpioBroker/ScorpioBroker/blob/prepare_ge/docs/en/source/testing.rst).

## Further resources

For more detailed explaination on NGSI-LD or JSON-LD. Please look at the
-  [ETSI Specification](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.01.01_60/gs_CIM009v010101p.pdf) 
-  [JSON-LD website](https://json-ld.org/)

You can find a set of example calls, as a Postman collection, in the Examples folder. These examples use 2 Variables

- gatewayServer, which has to be `<brokerIP>:<brokerPort>`. When using default settings locally it would be localhost:9090
- link, which is for the examples providing @context via the Link header. For the examples we host an example @context. Set link to https://raw.githubusercontent.com/ScorpioBroker/ScorpioBroker/master/Examples/index.json

## Acknowledgements
Part of the development has been founded by the EU in the AUTOPILOT project.

### EU Acknowledgetment
This activity has received funding from the European Union’s Horizon 2020 research and innovation programme under Grant Agreement No. 731993 (Autopilot), No. 814918 (Fed4IoT) and No. 767498 (MIDIH, Open Call (MoLe). 
<img src="https://raw.githubusercontent.com/ScorpioBroker/ScorpioBroker/master/img/flag_yellow_low.jpg" width="160">
- [AUTOPILOT project: Automated driving Progressed by Internet Of Things](https://autopilot-project.eu/) <img src="https://raw.githubusercontent.com/ScorpioBroker/ScorpioBroker/master/img/autopilot.png" width="160">
- [Fed4IoT project: (https://fed4iot.org/)
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

Scorpio is licensed under [BSD-4-Clause](https://spdx.org/licenses/BSD-4-Clause.html).

© 2019 NEC
